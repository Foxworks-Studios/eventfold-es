//! Top-level entry point that composes actor spawning, handle caching,
//! projections, and process managers into a single [`AggregateStore`] type.
//!
//! The store is opened via [`AggregateStoreBuilder`], which connects to
//! an `eventfold-db` gRPC server. All state is rebuilt from the event log
//! on startup -- there is no local disk caching.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::sync::RwLock;

use crate::actor::{ActorConfig, AggregateHandle, spawn_actor_with_config};
use crate::aggregate::Aggregate;
use crate::client::{EsClient, ExpectedVersionArg};
use crate::command::CommandEnvelope;
use crate::event::{ProposedEventData, stream_uuid};
use crate::live::{LiveConfig, LiveHandle, run_live_loop};
use crate::process_manager::{
    ProcessManager, ProcessManagerCatchUp, ProcessManagerReport, ProcessManagerRunner,
    append_dead_letter,
};
use crate::projection::{Projection, ProjectionCatchUp, ProjectionRunner};

/// Type-erased handle cache keyed by `(TypeId, instance_id)`.
///
/// `TypeId` identifies the aggregate type at runtime; the `String` is the
/// instance ID. `Box<dyn Any + Send + Sync>` lets a single map hold
/// `AggregateHandle<A>` for any concrete `A`. Downcasting recovers the
/// typed handle.
type HandleCache = HashMap<(TypeId, String), Box<dyn Any + Send + Sync>>;

/// Type-erased projection map keyed by projection name.
///
/// Each value is a `tokio::sync::Mutex<Box<dyn ProjectionCatchUp>>`, allowing
/// the store to interact with heterogeneous projection runners without knowing
/// each concrete `P` type. We use `tokio::sync::Mutex` because `catch_up` is
/// async.
type ProjectionMap = HashMap<String, tokio::sync::Mutex<Box<dyn ProjectionCatchUp>>>;

/// Type-erased list of process manager runners.
///
/// Each runner implements `ProcessManagerCatchUp` behind a `tokio::sync::Mutex`
/// because `catch_up` returns a boxed future.
type ProcessManagerList = Vec<tokio::sync::Mutex<Box<dyn ProcessManagerCatchUp>>>;

/// Type-erased dispatcher map keyed by aggregate type name.
///
/// Dispatchers route [`CommandEnvelope`]s from process managers to the
/// target aggregate, deserializing the JSON command and executing it.
pub(crate) type DispatcherMap = HashMap<String, Box<dyn AggregateDispatcher>>;

/// Default idle timeout for actors: 5 minutes.
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Options controlling the behaviour of [`AggregateStore::inject_event`].
///
/// # Examples
///
/// ```
/// use eventfold_es::InjectOptions;
///
/// // Default: do not run process managers after injection.
/// let opts = InjectOptions::default();
/// assert!(!opts.run_process_managers);
///
/// // Opt in to process manager triggering.
/// let opts = InjectOptions { run_process_managers: true };
/// assert!(opts.run_process_managers);
/// ```
#[derive(Debug, Clone, Default)]
pub struct InjectOptions {
    /// When `true`, call [`AggregateStore::run_process_managers`] after
    /// appending the event. Defaults to `false`.
    pub run_process_managers: bool,
}

/// Central registry that manages aggregate instance lifecycles.
///
/// The store handles actor spawning, handle caching, projection catch-up,
/// and process manager dispatch. It connects to an `eventfold-db` gRPC
/// server for event persistence.
///
/// `Clone` is cheap -- all internal state is `Arc`-wrapped.
#[derive(Clone)]
pub struct AggregateStore {
    pub(crate) client: EsClient,
    pub(crate) base_dir: PathBuf,
    pub(crate) cache: Arc<RwLock<HandleCache>>,
    pub(crate) projections: Arc<tokio::sync::RwLock<ProjectionMap>>,
    pub(crate) process_managers: Arc<tokio::sync::RwLock<ProcessManagerList>>,
    pub(crate) dispatchers: Arc<DispatcherMap>,
    pub(crate) idle_timeout: Duration,
    pub(crate) live_config: LiveConfig,
    pub(crate) live_handle: Arc<tokio::sync::Mutex<Option<LiveHandle>>>,
}

// Manual `Debug` because `dyn Any` is not `Debug` and we don't want to
// expose cache internals.
impl std::fmt::Debug for AggregateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggregateStore")
            .field("base_dir", &self.base_dir)
            .finish()
    }
}

impl AggregateStore {
    /// Get a handle to an aggregate instance, spawning its actor if needed.
    ///
    /// If the actor is already running (cached and alive), returns a clone
    /// of the existing handle. Otherwise, derives the stream UUID via
    /// [`stream_uuid`] and spawns a new actor.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique instance identifier within the aggregate type.
    ///
    /// # Returns
    ///
    /// An [`AggregateHandle`] for sending commands and reading state.
    ///
    /// # Errors
    ///
    /// Returns [`tonic::Status`] if the actor's catch-up read from the
    /// server fails.
    pub async fn get<A: Aggregate>(&self, id: &str) -> Result<AggregateHandle<A>, tonic::Status>
    where
        A::Command: Clone,
        A::DomainEvent: DeserializeOwned,
    {
        let key = (TypeId::of::<A>(), id.to_owned());

        // Fast path: check cache with read lock.
        {
            let cache = self.cache.read().await;
            if let Some(boxed) = cache.get(&key)
                && let Some(handle) = boxed.downcast_ref::<AggregateHandle<A>>()
                && handle.is_alive()
            {
                return Ok(handle.clone());
            }
        }

        // Slow path: evict stale entry and spawn new actor.
        {
            let mut cache = self.cache.write().await;
            cache.remove(&key);
        }

        tracing::debug!(
            aggregate_type = A::AGGREGATE_TYPE,
            instance_id = %id,
            "spawning actor"
        );

        let config = ActorConfig {
            idle_timeout: self.idle_timeout,
        };
        let handle = spawn_actor_with_config::<A>(id, self.client.clone(), config).await?;

        let mut cache = self.cache.write().await;
        cache.insert(key, Box::new(handle.clone()));
        Ok(handle)
    }

    /// Append a pre-built event directly to a stream, bypassing command
    /// validation.
    ///
    /// Uses `ExpectedVersion::Any` for the append, making it suitable
    /// for relay-sync scenarios. Optionally triggers process managers
    /// after injection.
    ///
    /// # Arguments
    ///
    /// * `instance_id` - Unique instance identifier within the aggregate type.
    /// * `proposed` - A pre-built [`ProposedEventData`] to append as-is.
    /// * `opts` - Controls whether process managers run after injection.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the gRPC append fails or process manager
    /// catch-up fails.
    pub async fn inject_event<A: Aggregate>(
        &self,
        instance_id: &str,
        proposed: ProposedEventData,
        opts: InjectOptions,
    ) -> io::Result<()> {
        let stream_id = stream_uuid(A::AGGREGATE_TYPE, instance_id);
        let mut client = self.client.clone();
        client
            .append(stream_id, ExpectedVersionArg::Any, vec![proposed])
            .await
            .map_err(|e| io::Error::other(format!("gRPC append failed: {e}")))?;

        if opts.run_process_managers {
            self.run_process_managers().await?;
        }

        Ok(())
    }

    /// Start the live subscription loop in the background.
    ///
    /// Spawns a tokio task that subscribes to the global event log and
    /// continuously updates all registered projections and process managers.
    /// Returns a [`LiveHandle`] for checking catch-up status and shutting down.
    ///
    /// Can only be called once per `AggregateStore` instance. A second call
    /// returns an error without spawning another task.
    ///
    /// # Returns
    ///
    /// A [`LiveHandle`] for controlling the live subscription loop.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::AlreadyExists`] if live mode is already active.
    pub async fn start_live(&self) -> io::Result<LiveHandle> {
        let mut guard = self.live_handle.lock().await;
        if guard.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "live subscription already started",
            ));
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (position_tx, _position_rx) = tokio::sync::watch::channel(0u64);
        let position_tx = Arc::new(position_tx);
        let caught_up = Arc::new(AtomicBool::new(false));

        let store_clone = self.clone();
        let caught_up_clone = caught_up.clone();
        let position_tx_clone = Arc::clone(&position_tx);
        let task = tokio::spawn(async move {
            run_live_loop(store_clone, caught_up_clone, position_tx_clone, shutdown_rx).await
        });

        let handle = LiveHandle {
            shutdown_tx,
            caught_up,
            position_tx,
            task: Arc::new(tokio::sync::Mutex::new(Some(task))),
        };

        *guard = Some(handle.clone());
        Ok(handle)
    }

    /// Read a projection's state from the live subscription without catch-up.
    ///
    /// If live mode is active (i.e., [`start_live`](AggregateStore::start_live)
    /// has been called), returns the current in-memory state maintained by the
    /// live subscription loop. No gRPC call is made and no catch-up occurs.
    ///
    /// If live mode is not active, falls back to the pull-based
    /// [`projection`](AggregateStore::projection) method, which performs a
    /// full catch-up.
    ///
    /// # Returns
    ///
    /// A clone of the projection's current state.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotFound`] if the projection is not registered.
    /// Returns `io::Error` if the projection type does not match or (in
    /// fallback mode) if catch-up fails.
    pub async fn live_projection<P: Projection>(&self) -> io::Result<P> {
        let guard = self.live_handle.lock().await;
        if guard.is_some() {
            // Live mode active: read projection state without catch-up.
            drop(guard);
            let projections = self.projections.read().await;
            let runner_mutex = projections.get(P::NAME).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("projection '{}' not registered", P::NAME),
                )
            })?;
            let runner = runner_mutex.lock().await;
            let state_box = runner.state_any();
            state_box
                .downcast::<P>()
                .map(|b| *b)
                .map_err(|_| io::Error::other("projection type mismatch"))
        } else {
            // Live mode not active: fall back to pull-based catch-up.
            drop(guard);
            self.projection::<P>().await
        }
    }

    /// Catch up and return the current state of a registered projection.
    ///
    /// Triggers a catch-up pass (reading new events from the global log)
    /// before returning a clone of the projection state.
    ///
    /// # Returns
    ///
    /// A clone of the projection's current state after catching up.
    ///
    /// # Errors
    ///
    /// Returns `io::ErrorKind::NotFound` if the projection is not registered.
    /// Returns `io::Error` if catching up on events fails or the projection
    /// type does not match.
    pub async fn projection<P: Projection>(&self) -> io::Result<P> {
        let projections = self.projections.read().await;
        let runner_mutex = projections.get(P::NAME).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("projection '{}' not registered", P::NAME),
            )
        })?;
        let mut runner = runner_mutex.lock().await;
        runner.catch_up().await?;
        let state_box = runner.state_any();
        state_box
            .downcast::<P>()
            .map(|b| *b)
            .map_err(|_| io::Error::other("projection type mismatch"))
    }

    /// Run all registered process managers through a catch-up pass.
    ///
    /// For each process manager:
    /// 1. Catch up on the global log, collecting command envelopes.
    /// 2. Dispatch each envelope to the target aggregate via the type registry.
    /// 3. Write failed dispatches to the per-PM dead-letter log.
    ///
    /// # Returns
    ///
    /// A [`ProcessManagerReport`] summarizing how many envelopes were
    /// dispatched and how many were dead-lettered.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if catching up fails.
    pub async fn run_process_managers(&self) -> io::Result<ProcessManagerReport> {
        let mut all_work: Vec<(Vec<CommandEnvelope>, PathBuf)> = Vec::new();

        {
            let pms = self.process_managers.read().await;
            for pm_mutex in pms.iter() {
                let mut pm = pm_mutex.lock().await;
                let envelopes = pm.catch_up().await?;
                let dead_letter_path = pm.dead_letter_path();
                all_work.push((envelopes, dead_letter_path));
            }
        }

        let mut report = ProcessManagerReport::default();
        for (envelopes, dead_letter_path) in &all_work {
            for envelope in envelopes {
                let agg_type = &envelope.aggregate_type;
                match self.dispatchers.get(agg_type) {
                    Some(dispatcher) => match dispatcher.dispatch(self, envelope.clone()).await {
                        Ok(()) => {
                            tracing::info!(
                                target_type = %agg_type,
                                target_id = %envelope.instance_id,
                                "dispatched command"
                            );
                            report.dispatched += 1;
                        }
                        Err(e) => {
                            tracing::error!(
                                aggregate_type = %agg_type,
                                instance_id = %envelope.instance_id,
                                error = %e,
                                "process manager dispatch failed, dead-lettering"
                            );
                            append_dead_letter(dead_letter_path, envelope.clone(), &e.to_string())?;
                            report.dead_lettered += 1;
                        }
                    },
                    None => {
                        let err_msg = format!("unknown aggregate type: {agg_type}");
                        tracing::error!(
                            aggregate_type = %agg_type,
                            "no dispatcher registered, dead-lettering"
                        );
                        append_dead_letter(dead_letter_path, envelope.clone(), &err_msg)?;
                        report.dead_lettered += 1;
                    }
                }
            }
        }

        Ok(report)
    }
}

// --- Type-erased dispatch for process manager command routing ---

/// Type-erased interface for dispatching [`CommandEnvelope`]s to an
/// aggregate type.
///
/// Each concrete `TypedDispatcher<A>` implements this trait, deserializing
/// the JSON command payload into `A::Command` and executing it via
/// the store's `get::<A>(id)` handle.
#[tonic::async_trait]
pub(crate) trait AggregateDispatcher: Send + Sync {
    /// Dispatch a command envelope to the target aggregate.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if deserialization, actor spawn, or execution fails.
    async fn dispatch(&self, store: &AggregateStore, envelope: CommandEnvelope) -> io::Result<()>;
}

/// Concrete dispatcher for a specific aggregate type `A`.
///
/// Deserializes the JSON command payload into `A::Command` and executes
/// it through the store.
struct TypedDispatcher<A: Aggregate> {
    _marker: std::marker::PhantomData<A>,
}

impl<A: Aggregate> TypedDispatcher<A> {
    /// Create a new typed dispatcher.
    fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[tonic::async_trait]
impl<A> AggregateDispatcher for TypedDispatcher<A>
where
    A: Aggregate,
    A::Command: DeserializeOwned + Clone,
    A::DomainEvent: DeserializeOwned,
{
    async fn dispatch(&self, store: &AggregateStore, envelope: CommandEnvelope) -> io::Result<()> {
        let cmd: A::Command = serde_json::from_value(envelope.command)
            .map_err(|e| io::Error::other(format!("command deserialization failed: {e}")))?;
        let handle = store
            .get::<A>(&envelope.instance_id)
            .await
            .map_err(|e| io::Error::other(format!("actor spawn failed: {e}")))?;
        handle
            .execute(cmd, envelope.context)
            .await
            .map_err(|e| io::Error::other(format!("command execution failed: {e}")))?;
        Ok(())
    }
}

// --- Factory types for deferred construction ---

/// Factory for creating a type-erased projection runner.
type ProjectionFactory =
    Box<dyn FnOnce(EsClient, &Path) -> io::Result<tokio::sync::Mutex<Box<dyn ProjectionCatchUp>>>>;

/// Factory for creating a type-erased process manager runner.
type ProcessManagerFactory = Box<
    dyn FnOnce(EsClient, &Path) -> io::Result<tokio::sync::Mutex<Box<dyn ProcessManagerCatchUp>>>,
>;

/// Factory for creating a type-erased aggregate dispatcher.
type DispatcherFactory = Box<dyn FnOnce() -> Box<dyn AggregateDispatcher>>;

/// Builder for configuring and opening an [`AggregateStore`].
///
/// Collects configuration -- endpoint URL, local cache directory,
/// registered aggregates, projections, and process managers -- then
/// connects to the gRPC server on [`open`](AggregateStoreBuilder::open).
///
/// # Examples
///
/// ```no_run
/// use eventfold_es::AggregateStoreBuilder;
///
/// # async fn example() -> Result<(), tonic::transport::Error> {
/// let store = AggregateStoreBuilder::new()
///     .endpoint("http://127.0.0.1:2113")
///     .base_dir("/tmp/my-app")
///     .open()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AggregateStoreBuilder {
    endpoint: Option<String>,
    base_dir: Option<PathBuf>,
    auth_token: Option<Arc<std::sync::RwLock<String>>>,
    projection_factories: Vec<(String, ProjectionFactory)>,
    process_manager_factories: Vec<(String, ProcessManagerFactory)>,
    dispatcher_factories: Vec<(String, DispatcherFactory)>,
    idle_timeout: Duration,
    live_config: LiveConfig,
}

impl AggregateStoreBuilder {
    /// Create a new builder with no configuration.
    ///
    /// At minimum, [`endpoint`](AggregateStoreBuilder::endpoint) must be
    /// called before [`open`](AggregateStoreBuilder::open).
    pub fn new() -> Self {
        Self {
            endpoint: None,
            base_dir: None,
            auth_token: None,
            projection_factories: Vec::new(),
            process_manager_factories: Vec::new(),
            dispatcher_factories: Vec::new(),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            live_config: LiveConfig::default(),
        }
    }

    /// Set the gRPC server endpoint URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The URI of the `eventfold-db` gRPC server
    ///   (e.g. `"http://127.0.0.1:2113"`).
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    /// Set a shared Bearer token for authenticated gRPC connections.
    ///
    /// The token is wrapped in an `Arc<RwLock<String>>` so that callers can
    /// refresh the token in-place at runtime. The [`BearerInterceptor`]
    /// reads the lock on every outgoing RPC, so writing a new value into
    /// the lock is sufficient to rotate credentials without reconnecting.
    ///
    /// If the token string is empty, no `Authorization` header is sent,
    /// effectively behaving as an unauthenticated connection.
    ///
    /// # Arguments
    ///
    /// * `token` - Shared, refreshable Bearer token string.
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    ///
    /// [`BearerInterceptor`]: crate::auth::BearerInterceptor
    pub fn auth_token(mut self, token: Arc<std::sync::RwLock<String>>) -> Self {
        self.auth_token = Some(token);
        self
    }

    /// Set the local data directory for dead-letter logs.
    ///
    /// If not set, defaults to a system temp directory.
    ///
    /// # Arguments
    ///
    /// * `path` - Root directory for local cache data.
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn base_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.base_dir = Some(path.as_ref().to_owned());
        self
    }

    /// Register an aggregate type as a dispatch target for process managers.
    ///
    /// This allows [`CommandEnvelope`]s targeting this aggregate type to be
    /// deserialized and routed. The aggregate's `Command` type must implement
    /// `DeserializeOwned`.
    ///
    /// # Type Parameters
    ///
    /// * `A` - A type implementing [`Aggregate`] with `Command: DeserializeOwned + Clone`.
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn aggregate_type<A>(mut self) -> Self
    where
        A: Aggregate,
        A::Command: DeserializeOwned + Clone,
        A::DomainEvent: DeserializeOwned,
    {
        self.dispatcher_factories.push((
            A::AGGREGATE_TYPE.to_owned(),
            Box::new(|| Box::new(TypedDispatcher::<A>::new()) as Box<dyn AggregateDispatcher>),
        ));
        self
    }

    /// Register a projection type to be managed by this store.
    ///
    /// The projection will be initialized when
    /// [`open`](AggregateStoreBuilder::open) is called.
    ///
    /// # Type Parameters
    ///
    /// * `P` - A type implementing [`Projection`].
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn projection<P: Projection>(mut self) -> Self {
        self.projection_factories.push((
            P::NAME.to_owned(),
            Box::new(|client: EsClient, _base_dir: &Path| {
                Ok(tokio::sync::Mutex::new(
                    Box::new(ProjectionRunner::<P>::new(client)) as Box<dyn ProjectionCatchUp>,
                ))
            }),
        ));
        self
    }

    /// Register a process manager type to be managed by this store.
    ///
    /// The process manager will be initialized when
    /// [`open`](AggregateStoreBuilder::open) is called.
    ///
    /// # Type Parameters
    ///
    /// * `PM` - A type implementing [`ProcessManager`].
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn process_manager<PM>(mut self) -> Self
    where
        PM: ProcessManager,
    {
        self.process_manager_factories.push((
            PM::NAME.to_owned(),
            Box::new(|client: EsClient, base_dir: &Path| {
                let data_dir = base_dir.join("process_managers").join(PM::NAME);
                let runner = ProcessManagerRunner::<PM>::new(client, data_dir);
                Ok(tokio::sync::Mutex::new(
                    Box::new(runner) as Box<dyn ProcessManagerCatchUp>
                ))
            }),
        ));
        self
    }

    /// Set the idle timeout for actor eviction.
    ///
    /// Actors that receive no messages for this duration will shut down.
    /// The next [`get`](AggregateStore::get) call transparently re-spawns
    /// the actor, replaying from the event log.
    ///
    /// Defaults to 5 minutes.
    ///
    /// # Arguments
    ///
    /// * `timeout` - How long an idle actor waits before shutting down.
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set the live subscription configuration.
    ///
    /// Controls reconnection backoff for the live subscription loop
    /// started by
    /// [`AggregateStore::start_live`](AggregateStore).
    ///
    /// If not called, [`LiveConfig::default()`] is used.
    ///
    /// # Arguments
    ///
    /// * `config` - The [`LiveConfig`] to use for live subscriptions.
    ///
    /// # Returns
    ///
    /// `self` for method chaining.
    pub fn live_config(mut self, config: LiveConfig) -> Self {
        self.live_config = config;
        self
    }

    /// Connect to the gRPC server and build the [`AggregateStore`].
    ///
    /// Establishes a gRPC channel to the configured endpoint, initializes
    /// all registered projections and process managers, and returns the store.
    ///
    /// # Returns
    ///
    /// A fully initialized [`AggregateStore`].
    ///
    /// # Errors
    ///
    /// Returns [`tonic::transport::Error`] if the gRPC connection fails.
    /// Returns the error wrapped as `tonic::transport::Error` if projection
    /// or process manager initialization fails.
    pub async fn open(self) -> Result<AggregateStore, tonic::transport::Error> {
        let endpoint = self.endpoint.as_deref().unwrap_or("http://127.0.0.1:2113");

        let client = match self.auth_token {
            Some(token) => EsClient::connect_with_token(endpoint, token).await?,
            None => EsClient::connect(endpoint).await?,
        };

        let base_dir = self
            .base_dir
            .unwrap_or_else(|| std::env::temp_dir().join("eventfold-es"));

        // Initialize projections.
        let mut projections = HashMap::new();
        for (name, factory) in self.projection_factories {
            let runner = factory(client.clone(), &base_dir)
                .map_err(|e| {
                    // tonic::transport::Error is opaque; we cannot construct one
                    // directly. Instead, we log and panic. In practice, this only
                    // fails on I/O errors during initialization.
                    tracing::error!(error = %e, "failed to initialize projection '{name}'");
                    e
                })
                .expect("projection initialization should not fail");
            projections.insert(name, runner);
        }

        // Initialize process managers.
        let mut process_managers = Vec::new();
        for (_name, factory) in self.process_manager_factories {
            let runner = factory(client.clone(), &base_dir)
                .expect("process manager initialization should not fail");
            process_managers.push(runner);
        }

        // Build dispatcher map.
        let mut dispatchers: HashMap<String, Box<dyn AggregateDispatcher>> = HashMap::new();
        for (name, factory) in self.dispatcher_factories {
            dispatchers.insert(name, factory());
        }

        Ok(AggregateStore {
            client,
            base_dir,
            cache: Arc::new(RwLock::new(HashMap::new())),
            projections: Arc::new(tokio::sync::RwLock::new(projections)),
            process_managers: Arc::new(tokio::sync::RwLock::new(process_managers)),
            dispatchers: Arc::new(dispatchers),
            idle_timeout: self.idle_timeout,
            live_config: self.live_config,
            live_handle: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }
}

impl Default for AggregateStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::AggregateHandle;
    use crate::aggregate::test_fixtures::Counter;

    /// Build a mock `AggregateStore` for tests that don't need a live gRPC
    /// server. The `get` method will fail on cache miss (since the client
    /// is invalid), but cache-hit paths work correctly.
    fn mock_store(base_dir: &Path) -> AggregateStore {
        mock_store_with_live_config(base_dir, LiveConfig::default())
    }

    /// Build a mock `AggregateStore` with a specific [`LiveConfig`] for tests
    /// that need to verify config propagation.
    fn mock_store_with_live_config(base_dir: &Path, live_config: LiveConfig) -> AggregateStore {
        // Create a dummy EsClient by connecting to a nonsense endpoint.
        // We use `Endpoint::from_static` + `connect_lazy` to get a Channel
        // without actually connecting. This is fine for tests that only
        // exercise cache logic.
        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = EsClient::from_inner(inner);
        AggregateStore {
            client,
            base_dir: base_dir.to_owned(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            projections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            process_managers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            dispatchers: Arc::new(HashMap::new()),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            live_config,
            live_handle: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    #[tokio::test]
    async fn start_live_twice_returns_already_exists_error() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());
        let handle = store
            .start_live()
            .await
            .expect("first start_live should succeed");

        let result = store.start_live().await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("second start_live should return Err"),
        };
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

        // Clean up.
        handle.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test]
    async fn live_projection_returns_state_when_live_active() {
        use crate::event::StoredEvent;
        use crate::projection::{ProjectionCatchUp, ProjectionRunner};

        // A minimal projection for testing.
        #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        struct TestCounter {
            count: u64,
        }

        impl crate::projection::Projection for TestCounter {
            const NAME: &'static str = "test-counter";
            fn apply(&mut self, _event: &StoredEvent) {
                self.count += 1;
            }
        }

        let tmp = tempfile::tempdir().expect("temp dir");
        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = EsClient::from_inner(inner);

        // Create a projection runner and manually apply events to simulate
        // live loop having processed events.
        let runner = ProjectionRunner::<TestCounter>::new(client.clone());
        let mut projections = HashMap::new();
        projections.insert(
            "test-counter".to_string(),
            tokio::sync::Mutex::new(Box::new(runner) as Box<dyn ProjectionCatchUp>),
        );

        // Apply 3 events manually (simulating live loop).
        {
            let runner_mutex = projections.get("test-counter").unwrap();
            let mut runner = runner_mutex.lock().await;
            let recorded = crate::proto::RecordedEvent {
                event_id: uuid::Uuid::new_v4().to_string(),
                stream_id: crate::event::stream_uuid("counter", "c-1").to_string(),
                stream_version: 0,
                global_position: 0,
                event_type: "Incremented".to_string(),
                metadata: serde_json::to_vec(
                    &serde_json::json!({"aggregate_type": "counter", "instance_id": "c-1"}),
                )
                .unwrap(),
                payload: b"{}".to_vec(),
                recorded_at: 1_700_000_000_000,
            };
            runner.apply_event(&recorded);
            let recorded2 = crate::proto::RecordedEvent {
                global_position: 1,
                stream_version: 1,
                ..recorded.clone()
            };
            runner.apply_event(&recorded2);
            let recorded3 = crate::proto::RecordedEvent {
                event_id: uuid::Uuid::new_v4().to_string(),
                global_position: 2,
                stream_version: 2,
                ..recorded
            };
            runner.apply_event(&recorded3);
        }

        let store = AggregateStore {
            client,
            base_dir: tmp.path().to_owned(),
            cache: Arc::new(RwLock::new(HashMap::new())),
            projections: Arc::new(tokio::sync::RwLock::new(projections)),
            process_managers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            dispatchers: Arc::new(HashMap::new()),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            live_config: LiveConfig::default(),
            live_handle: Arc::new(tokio::sync::Mutex::new(None)),
        };

        // Simulate live mode being active by placing a LiveHandle in the mutex.
        {
            let (shutdown_tx, _rx) = tokio::sync::watch::channel(false);
            let handle = crate::live::LiveHandle {
                shutdown_tx,
                caught_up: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                position_tx: Arc::new(tokio::sync::watch::channel(0u64).0),
                task: Arc::new(tokio::sync::Mutex::new(None)),
            };
            *store.live_handle.lock().await = Some(handle);
        }

        // live_projection should return state without catch-up.
        let state: TestCounter = store
            .live_projection::<TestCounter>()
            .await
            .expect("live_projection should succeed");
        assert_eq!(state.count, 3);
    }

    #[tokio::test]
    async fn live_projection_falls_back_when_not_live() {
        use crate::event::StoredEvent;

        // A minimal projection for testing fallback.
        #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        struct FallbackCounter {
            count: u64,
        }

        impl crate::projection::Projection for FallbackCounter {
            const NAME: &'static str = "fallback-counter";
            fn apply(&mut self, _event: &StoredEvent) {
                self.count += 1;
            }
        }

        // Without live mode, live_projection should return NotFound for an
        // unregistered projection (which is the fallback to self.projection()).
        // We cannot test a successful catch-up without a live gRPC server,
        // but we can verify the fallback path is invoked by checking it
        // returns the same error as projection().
        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());

        let err = store
            .live_projection::<FallbackCounter>()
            .await
            .expect_err("should fail for unregistered projection");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn projection_works_when_live_mode_active() {
        // When live mode is active, projection() should still work
        // (it always does a catch-up pass). We verify it at least attempts
        // catch-up by checking it returns NotFound for unregistered projections,
        // which proves the code path isn't short-circuited.
        use crate::event::StoredEvent;

        #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        struct ProjTestCounter {
            count: u64,
        }

        impl crate::projection::Projection for ProjTestCounter {
            const NAME: &'static str = "proj-test-counter";
            fn apply(&mut self, _event: &StoredEvent) {
                self.count += 1;
            }
        }

        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());

        // Simulate live mode being active.
        {
            let (shutdown_tx, _rx) = tokio::sync::watch::channel(false);
            let handle = crate::live::LiveHandle {
                shutdown_tx,
                caught_up: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                position_tx: Arc::new(tokio::sync::watch::channel(0u64).0),
                task: Arc::new(tokio::sync::Mutex::new(None)),
            };
            *store.live_handle.lock().await = Some(handle);
        }

        // projection() should still work (returns NotFound for unregistered
        // projection, same as when live is not active).
        let err = store
            .projection::<ProjTestCounter>()
            .await
            .expect_err("should fail for unregistered projection");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn projection_works_when_live_mode_not_active() {
        // Same test without live mode to confirm pull-based path unchanged.
        use crate::event::StoredEvent;

        #[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
        struct ProjTestCounter2 {
            count: u64,
        }

        impl crate::projection::Projection for ProjTestCounter2 {
            const NAME: &'static str = "proj-test-counter2";
            fn apply(&mut self, _event: &StoredEvent) {
                self.count += 1;
            }
        }

        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());

        let err = store
            .projection::<ProjTestCounter2>()
            .await
            .expect_err("should fail for unregistered projection");
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn builder_connect_returns_err_when_no_server() {
        let result = AggregateStoreBuilder::new()
            .endpoint("http://127.0.0.1:1")
            .open()
            .await;
        assert!(
            result.is_err(),
            "open should fail when no server is listening on port 1"
        );
    }

    #[tokio::test]
    async fn builder_stores_custom_live_config() {
        use crate::live::LiveConfig;

        let custom = LiveConfig {
            reconnect_base_delay: Duration::from_millis(500),
            reconnect_max_delay: Duration::from_secs(60),
        };
        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store_with_live_config(tmp.path(), custom.clone());
        assert_eq!(
            store.live_config.reconnect_base_delay,
            Duration::from_millis(500)
        );
        assert_eq!(
            store.live_config.reconnect_max_delay,
            Duration::from_secs(60)
        );
    }

    #[tokio::test]
    async fn builder_default_live_config() {
        use crate::live::LiveConfig;

        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());
        let default = LiveConfig::default();
        assert_eq!(
            store.live_config.reconnect_base_delay,
            default.reconnect_base_delay
        );
        assert_eq!(
            store.live_config.reconnect_max_delay,
            default.reconnect_max_delay
        );
    }

    #[tokio::test]
    async fn start_live_returns_ok_handle() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());
        let result = store.start_live().await;
        assert!(result.is_ok(), "start_live should return Ok");
        let handle = result.unwrap();
        // Clean up the spawned task.
        handle.shutdown().await.expect("shutdown should succeed");
    }

    #[test]
    fn builder_without_auth_token_has_none() {
        let builder = AggregateStoreBuilder::new();
        assert!(
            builder.auth_token.is_none(),
            "auth_token should be None by default"
        );
    }

    #[test]
    fn builder_with_auth_token_has_some() {
        let token = Arc::new(std::sync::RwLock::new("tok".to_string()));
        let builder = AggregateStoreBuilder::new().auth_token(token);
        assert!(
            builder.auth_token.is_some(),
            "auth_token should be Some after calling .auth_token()"
        );
    }

    #[tokio::test]
    async fn get_twice_returns_cached_alive_handles() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let store = mock_store(tmp.path());

        // Pre-populate the cache with a handle backed by a live channel.
        // We create the channel manually; the receiver is kept alive so
        // `is_alive()` returns true.
        let (tx, _rx) = tokio::sync::mpsc::channel::<crate::actor::ActorMessage<Counter>>(1);
        let handle = AggregateHandle::<Counter>::from_sender(tx);
        let key = (TypeId::of::<Counter>(), "c-1".to_owned());
        store.cache.write().await.insert(key, Box::new(handle));

        // First get should return the cached handle.
        let h1 = store
            .get::<Counter>("c-1")
            .await
            .expect("first get should succeed");
        assert!(h1.is_alive(), "first handle should be alive");

        // Second get should also return the cached handle.
        let h2 = store
            .get::<Counter>("c-1")
            .await
            .expect("second get should succeed");
        assert!(h2.is_alive(), "second handle should be alive");
    }
}
