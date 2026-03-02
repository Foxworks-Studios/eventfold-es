//! Actor loop that owns an aggregate and processes commands via gRPC.
//!
//! The actor runs as a tokio task and sequentially processes messages
//! from an `mpsc` channel. It holds the aggregate state, stream version,
//! and stream UUID, communicating with the event store through the
//! [`EventStoreOps`] trait (implemented by [`EsClient`] for production use).
//!
//! Public API: [`AggregateHandle`] (cloneable async handle) and
//! [`spawn_actor_with_config`] (factory that replays the full event stream
//! and starts the actor task).

use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;
use uuid::Uuid;

use crate::aggregate::Aggregate;
use crate::client::ExpectedVersionArg;
use crate::command::CommandContext;
use crate::error::{ExecuteError, StateError};
use crate::event::{ProposedEventData, encode_domain_event, stream_uuid};
use crate::proto::RecordedEvent;

/// Maximum number of optimistic concurrency retries before giving up.
const MAX_RETRIES: u32 = 3;

/// Abstraction over event store operations for testability.
///
/// [`EsClient`](crate::EsClient) implements this trait for production use.
/// Tests can provide a mock implementation without starting a gRPC server.
#[tonic::async_trait]
pub(crate) trait EventStoreOps: Send + Sync + 'static {
    /// Append events to a stream with optimistic concurrency control.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Target stream UUID.
    /// * `expected` - Expected stream version for OCC.
    /// * `events` - Events to append.
    ///
    /// # Errors
    ///
    /// Returns `tonic::Status` on failure (e.g. `FAILED_PRECONDITION` for
    /// version conflicts).
    async fn append(
        &mut self,
        stream_id: Uuid,
        expected: ExpectedVersionArg,
        events: Vec<ProposedEventData>,
    ) -> Result<crate::proto::AppendResponse, tonic::Status>;

    /// Read events from a stream starting at a given version.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - The stream UUID.
    /// * `from_version` - Zero-based version to start reading from.
    /// * `max_count` - Maximum number of events to return.
    ///
    /// # Errors
    ///
    /// Returns `tonic::Status` on failure.
    async fn read_stream(
        &mut self,
        stream_id: Uuid,
        from_version: u64,
        max_count: u64,
    ) -> Result<Vec<RecordedEvent>, tonic::Status>;
}

#[tonic::async_trait]
impl EventStoreOps for crate::client::EsClient {
    async fn append(
        &mut self,
        stream_id: Uuid,
        expected: ExpectedVersionArg,
        events: Vec<ProposedEventData>,
    ) -> Result<crate::proto::AppendResponse, tonic::Status> {
        self.append(stream_id, expected, events).await
    }

    async fn read_stream(
        &mut self,
        stream_id: Uuid,
        from_version: u64,
        max_count: u64,
    ) -> Result<Vec<RecordedEvent>, tonic::Status> {
        self.read_stream(stream_id, from_version, max_count).await
    }
}

/// Configuration for the actor loop.
///
/// Internal to the crate -- callers configure idle timeout through
/// [`AggregateStoreBuilder::idle_timeout`](crate::AggregateStoreBuilder::idle_timeout).
pub(crate) struct ActorConfig {
    /// How long the actor waits for a message before shutting down.
    pub idle_timeout: Duration,
}

/// Result type sent back through the `Execute` reply channel.
type ExecuteResult<A> =
    Result<Vec<<A as Aggregate>::DomainEvent>, ExecuteError<<A as Aggregate>::Error>>;

/// Messages sent from `AggregateHandle` to the actor loop.
///
/// Each variant carries a `oneshot::Sender` for the actor to reply on
/// once the operation completes.
pub(crate) enum ActorMessage<A: Aggregate> {
    /// Execute a command against the aggregate.
    Execute {
        /// The domain command to execute.
        cmd: A::Command,
        /// Cross-cutting metadata (actor identity, correlation ID, etc.).
        ctx: CommandContext,
        /// Channel to send back the produced domain events or an error.
        reply: oneshot::Sender<ExecuteResult<A>>,
    },

    /// Retrieve the current aggregate state.
    GetState {
        /// Channel to send back a clone of the current state or an error.
        reply: oneshot::Sender<Result<A, StateError>>,
    },

    /// Inject a pre-built [`ProposedEventData`] directly into the stream.
    ///
    /// The actor appends with `ExpectedVersion::Any` and sends the result
    /// back on `reply`.
    #[allow(dead_code)] // Constructed via inject_via_actor; used in tests.
    Inject {
        /// The proposed event to append.
        proposed: ProposedEventData,
        /// Channel to send back the gRPC result.
        reply: oneshot::Sender<Result<(), tonic::Status>>,
    },

    /// Gracefully shut down the actor loop.
    #[allow(dead_code)] // Used in tests for controlled shutdown.
    Shutdown,
}

/// Async handle to a running aggregate actor.
///
/// Lightweight, cloneable, and `Send + Sync`. Communicates with the
/// actor task over a bounded channel.
///
/// # Type Parameters
///
/// * `A` - The [`Aggregate`] type this handle controls.
#[derive(Debug)]
pub struct AggregateHandle<A: Aggregate> {
    sender: mpsc::Sender<ActorMessage<A>>,
}

// Manual `Clone` because `A` itself need not be `Clone` for the handle --
// we only clone the `Sender`, which is always `Clone` regardless of `A`.
impl<A: Aggregate> Clone for AggregateHandle<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl<A: Aggregate> AggregateHandle<A> {
    /// Send a command to the aggregate and wait for the result.
    ///
    /// Returns the domain events produced by the command on success.
    ///
    /// # Arguments
    ///
    /// * `cmd` - The domain command to execute against the aggregate.
    /// * `ctx` - Cross-cutting metadata (actor identity, correlation ID, etc.).
    ///
    /// # Returns
    ///
    /// The domain events produced by the command on success.
    ///
    /// # Errors
    ///
    /// * [`ExecuteError::Domain`] -- the aggregate rejected the command.
    /// * [`ExecuteError::WrongExpectedVersion`] -- retries exhausted after
    ///   concurrent version conflicts.
    /// * [`ExecuteError::Transport`] -- a gRPC error occurred.
    /// * [`ExecuteError::ActorGone`] -- the actor task has exited.
    pub async fn execute(
        &self,
        cmd: A::Command,
        ctx: CommandContext,
    ) -> Result<Vec<A::DomainEvent>, ExecuteError<A::Error>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ActorMessage::Execute {
                cmd,
                ctx,
                reply: tx,
            })
            .await
            .map_err(|_| ExecuteError::ActorGone)?;
        rx.await.map_err(|_| ExecuteError::ActorGone)?
    }

    /// Read the current aggregate state.
    ///
    /// # Returns
    ///
    /// A clone of the current aggregate state.
    ///
    /// # Errors
    ///
    /// * [`StateError::ActorGone`] -- the actor task has exited.
    pub async fn state(&self) -> Result<A, StateError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ActorMessage::GetState { reply: tx })
            .await
            .map_err(|_| StateError::ActorGone)?;
        rx.await.map_err(|_| StateError::ActorGone)?
    }

    /// Inject a pre-built [`ProposedEventData`] by sending it to the actor.
    ///
    /// The actor appends with `ExpectedVersion::Any` and returns the gRPC
    /// result.
    ///
    /// # Arguments
    ///
    /// * `proposed` - A pre-built proposed event to append as-is.
    ///
    /// # Errors
    ///
    /// * [`tonic::Status`] if the gRPC append fails.
    /// * `tonic::Status` with `CANCELLED` code if the actor has exited.
    #[allow(dead_code)] // Actor-based injection path; used in tests.
    pub(crate) async fn inject_via_actor(
        &self,
        proposed: ProposedEventData,
    ) -> Result<(), tonic::Status> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(ActorMessage::Inject {
                proposed,
                reply: tx,
            })
            .await
            .map_err(|_| tonic::Status::cancelled("actor gone"))?;
        rx.await
            .map_err(|_| tonic::Status::cancelled("actor gone"))?
    }

    /// Check whether the actor backing this handle is still running.
    ///
    /// Returns `false` if the actor task has exited (e.g. due to idle
    /// timeout or shutdown). The store uses this to evict stale handles
    /// from its cache and re-spawn the actor on the next `get` call.
    pub fn is_alive(&self) -> bool {
        !self.sender.is_closed()
    }

    /// Construct an `AggregateHandle` from a raw sender.
    ///
    /// Used in tests to create handles without spawning an actor task.
    #[cfg(test)]
    pub(crate) fn from_sender(sender: mpsc::Sender<ActorMessage<A>>) -> Self {
        Self { sender }
    }
}

/// Decode a [`RecordedEvent`] payload into a domain event for folding.
///
/// Reconstructs the adjacently-tagged JSON (`{"type": ..., "data": ...}`)
/// from the event's `event_type` and `payload` fields, then deserializes
/// into the aggregate's `DomainEvent` type.
fn decode_domain_event<A: Aggregate>(recorded: &RecordedEvent) -> Option<A::DomainEvent>
where
    A::DomainEvent: DeserializeOwned,
{
    // Reconstruct the adjacently-tagged envelope that serde expects.
    let payload_str = std::str::from_utf8(&recorded.payload).ok()?;
    let payload_value: serde_json::Value = if payload_str.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(payload_str).ok()?
    };

    let envelope = if payload_value.is_null() {
        // Unit variant: {"type": "Incremented"}
        serde_json::json!({"type": recorded.event_type})
    } else {
        // Data variant: {"type": "Added", "data": {"amount": 42}}
        serde_json::json!({"type": recorded.event_type, "data": payload_value})
    };

    serde_json::from_value(envelope).ok()
}

/// Bundled context passed to the actor loop.
///
/// Groups the immutable identity fields and storage references so that
/// `run_actor` stays within the parameter limit.
struct ActorContext<S> {
    store: S,
    stream_id: Uuid,
    instance_id: String,
    config: ActorConfig,
}

/// Run the actor loop as an async task.
///
/// Processes messages from the channel, executing commands with optimistic
/// concurrency retries.
async fn run_actor<A, S>(
    mut ctx: ActorContext<S>,
    mut state: A,
    mut stream_version: Option<u64>,
    mut rx: mpsc::Receiver<ActorMessage<A>>,
) where
    A: Aggregate,
    A::Command: Clone,
    A::DomainEvent: DeserializeOwned,
    S: EventStoreOps,
{
    loop {
        let idle_timeout = ctx.config.idle_timeout;
        let msg = tokio::time::timeout(idle_timeout, rx.recv()).await;

        match msg {
            Ok(Some(msg)) => match msg {
                ActorMessage::Execute {
                    cmd,
                    ctx: cmd_ctx,
                    reply,
                } => {
                    let span = tracing::info_span!("execute", aggregate_type = A::AGGREGATE_TYPE);
                    let result = execute_with_retry::<A, S>(
                        &mut ctx.store,
                        &mut state,
                        &mut stream_version,
                        ctx.stream_id,
                        &ctx.instance_id,
                        cmd,
                        &cmd_ctx,
                    )
                    .instrument(span)
                    .await;
                    let _ = reply.send(result);
                }

                ActorMessage::GetState { reply } => {
                    let _ = reply.send(Ok(state.clone()));
                }

                ActorMessage::Inject { proposed, reply } => {
                    let result = ctx
                        .store
                        .append(ctx.stream_id, ExpectedVersionArg::Any, vec![proposed])
                        .await;
                    match result {
                        Ok(_resp) => {
                            // Re-read to update local state after injection.
                            let from = stream_version.map_or(0, |v| v + 1);
                            if let Ok(events) =
                                ctx.store.read_stream(ctx.stream_id, from, u64::MAX).await
                            {
                                for ev in &events {
                                    if let Some(domain_event) = decode_domain_event::<A>(ev) {
                                        state = state.apply(&domain_event);
                                    }
                                    stream_version = Some(ev.stream_version);
                                }
                            }
                            let _ = reply.send(Ok(()));
                        }
                        Err(status) => {
                            let _ = reply.send(Err(status));
                        }
                    }
                }

                ActorMessage::Shutdown => {
                    break;
                }
            },
            // Channel closed: all senders dropped.
            Ok(None) => {
                break;
            }
            // Idle timeout elapsed.
            Err(_elapsed) => {
                tracing::info!(
                    aggregate_type = A::AGGREGATE_TYPE,
                    "actor idle, shutting down"
                );
                break;
            }
        }
    }
}

/// Execute a command with optimistic concurrency retry.
///
/// On `FAILED_PRECONDITION`, re-reads events from the server, re-folds
/// state, and retries up to [`MAX_RETRIES`] times.
async fn execute_with_retry<A, S>(
    store: &mut S,
    state: &mut A,
    stream_version: &mut Option<u64>,
    stream_id: Uuid,
    instance_id: &str,
    cmd: A::Command,
    ctx: &CommandContext,
) -> Result<Vec<A::DomainEvent>, ExecuteError<A::Error>>
where
    A: Aggregate,
    A::DomainEvent: DeserializeOwned,
    S: EventStoreOps,
    A::Command: Clone,
{
    for attempt in 0..MAX_RETRIES {
        // 1. Decide: run the command handler against current state.
        let domain_events = state
            .clone()
            .handle(cmd.clone())
            .map_err(ExecuteError::Domain)?;

        // No-op commands produce no events.
        if domain_events.is_empty() {
            return Ok(domain_events);
        }

        // 2. Encode domain events for gRPC submission.
        let proposed: Vec<ProposedEventData> = domain_events
            .iter()
            .map(|de| encode_domain_event::<A>(de, ctx, A::AGGREGATE_TYPE, instance_id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                ExecuteError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;

        // 3. Append with OCC: NoStream for new streams, Exact(v) for existing.
        let expected = match *stream_version {
            Some(v) => ExpectedVersionArg::Exact(v),
            None => ExpectedVersionArg::NoStream,
        };
        match store.append(stream_id, expected, proposed).await {
            Ok(resp) => {
                // Success: fold events into local state, advance version.
                for de in &domain_events {
                    *state = state.clone().apply(de);
                }
                *stream_version = Some(resp.last_stream_version);
                tracing::info!(count = domain_events.len(), "events appended");
                return Ok(domain_events);
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                tracing::warn!(
                    attempt = attempt + 1,
                    max = MAX_RETRIES,
                    "version conflict, re-reading and retrying"
                );
                // Re-read from server to catch up with concurrent writes.
                let from = stream_version.map_or(0, |v| v + 1);
                let events = store
                    .read_stream(stream_id, from, u64::MAX)
                    .await
                    .map_err(ExecuteError::Transport)?;
                for ev in &events {
                    if let Some(domain_event) = decode_domain_event::<A>(ev) {
                        *state = state.clone().apply(&domain_event);
                    }
                    *stream_version = Some(ev.stream_version);
                }
                // Loop back and retry the command against updated state.
            }
            Err(status) => {
                return Err(ExecuteError::Transport(status));
            }
        }
    }

    // All retries exhausted.
    Err(ExecuteError::WrongExpectedVersion)
}

/// Spawn a new aggregate actor with explicit configuration.
///
/// Derives the stream UUID, replays the full event stream from the
/// server, then starts the actor loop as a tokio task.
///
/// # Arguments
///
/// * `instance_id` - The aggregate instance identifier (e.g. "c-1").
/// * `client` - The gRPC client for communicating with eventfold-db.
/// * `config` - Actor configuration (idle timeout).
///
/// # Returns
///
/// An [`AggregateHandle`] for sending commands and reading state.
///
/// # Errors
///
/// Returns [`tonic::Status`] if the catch-up read fails.
pub(crate) async fn spawn_actor_with_config<A: Aggregate>(
    instance_id: &str,
    client: crate::client::EsClient,
    config: ActorConfig,
) -> Result<AggregateHandle<A>, tonic::Status>
where
    A::Command: Clone,
    A::DomainEvent: DeserializeOwned,
{
    spawn_actor_with_store(instance_id, client, config).await
}

/// Internal spawn helper that is generic over the store implementation.
///
/// This enables tests to inject a mock [`EventStoreOps`] without needing
/// a real gRPC server.
async fn spawn_actor_with_store<A, S>(
    instance_id: &str,
    mut store: S,
    config: ActorConfig,
) -> Result<AggregateHandle<A>, tonic::Status>
where
    A: Aggregate,
    A::Command: Clone,
    A::DomainEvent: DeserializeOwned,
    S: EventStoreOps,
{
    let stream_id = stream_uuid(A::AGGREGATE_TYPE, instance_id);

    // Always replay the full event stream from the beginning.
    let mut state = A::default();
    let mut version: Option<u64> = None;

    let events = store.read_stream(stream_id, 0, u64::MAX).await?;
    for ev in &events {
        if let Some(domain_event) = decode_domain_event::<A>(ev) {
            state = state.apply(&domain_event);
        }
        version = Some(ev.stream_version);
    }

    let (tx, rx) = mpsc::channel::<ActorMessage<A>>(32);

    let actor_ctx = ActorContext {
        store,
        stream_id,
        instance_id: instance_id.to_string(),
        config,
    };

    tokio::spawn(async move {
        run_actor::<A, S>(actor_ctx, state, version, rx).await;
    });

    Ok(AggregateHandle { sender: tx })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::aggregate::test_fixtures::{Counter, CounterCommand};
    use crate::proto::AppendResponse;

    /// A configurable mock event store for testing.
    ///
    /// Stores events in memory and allows configuring append behavior
    /// (e.g. returning FAILED_PRECONDITION for version conflict tests).
    #[derive(Clone)]
    struct MockStore {
        /// Shared mutable state for the mock store.
        inner: Arc<Mutex<MockStoreInner>>,
    }

    struct MockStoreInner {
        /// Events stored in this mock, keyed by stream.
        events: Vec<RecordedEvent>,
        /// Number of FAILED_PRECONDITION errors to return before succeeding.
        precondition_failures_remaining: u32,
        /// Next stream version to assign.
        next_version: u64,
    }

    impl MockStore {
        /// Create a new mock store with no precondition failures.
        fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockStoreInner {
                    events: Vec::new(),
                    precondition_failures_remaining: 0,
                    next_version: 0,
                })),
            }
        }

        /// Create a mock store that fails the first N appends with
        /// FAILED_PRECONDITION.
        fn with_precondition_failures(n: u32) -> Self {
            Self {
                inner: Arc::new(Mutex::new(MockStoreInner {
                    events: Vec::new(),
                    precondition_failures_remaining: n,
                    next_version: 0,
                })),
            }
        }

        /// Seed the mock with pre-existing events.
        fn with_events(self, events: Vec<RecordedEvent>) -> Self {
            let mut inner = self.inner.lock().expect("lock mock store");
            inner.next_version = events.last().map_or(0, |e| e.stream_version + 1);
            inner.events = events;
            drop(inner);
            self
        }
    }

    #[tonic::async_trait]
    impl EventStoreOps for MockStore {
        async fn append(
            &mut self,
            _stream_id: Uuid,
            _expected: ExpectedVersionArg,
            events: Vec<ProposedEventData>,
        ) -> Result<AppendResponse, tonic::Status> {
            let mut inner = self.inner.lock().expect("lock mock store");

            if inner.precondition_failures_remaining > 0 {
                inner.precondition_failures_remaining -= 1;
                return Err(tonic::Status::failed_precondition("wrong expected version"));
            }

            // Record the events.
            for proposed in &events {
                let recorded = RecordedEvent {
                    event_id: proposed.event_id.to_string(),
                    stream_id: _stream_id.to_string(),
                    stream_version: inner.next_version,
                    global_position: inner.events.len() as u64,
                    event_type: proposed.event_type.clone(),
                    payload: serde_json::to_vec(&proposed.payload).unwrap_or_default(),
                    metadata: serde_json::to_vec(&proposed.metadata).unwrap_or_default(),
                    recorded_at: 1_700_000_000_000,
                };
                inner.events.push(recorded);
                inner.next_version += 1;
            }

            Ok(AppendResponse {
                first_stream_version: inner.next_version - events.len() as u64,
                last_stream_version: inner.next_version - 1,
                first_global_position: 0,
                last_global_position: 0,
            })
        }

        async fn read_stream(
            &mut self,
            _stream_id: Uuid,
            from_version: u64,
            max_count: u64,
        ) -> Result<Vec<RecordedEvent>, tonic::Status> {
            let inner = self.inner.lock().expect("lock mock store");
            let result: Vec<RecordedEvent> = inner
                .events
                .iter()
                .filter(|e| e.stream_version >= from_version)
                .take(max_count as usize)
                .cloned()
                .collect();
            Ok(result)
        }
    }

    /// Helper to spawn a test actor with a mock store.
    async fn spawn_test_actor(store: MockStore, instance_id: &str) -> AggregateHandle<Counter> {
        let config = ActorConfig {
            idle_timeout: Duration::from_secs(u64::MAX / 2),
        };
        spawn_actor_with_store::<Counter, MockStore>(instance_id, store, config)
            .await
            .expect("spawn_actor_with_store should succeed")
    }

    // ---- Test: 3x FAILED_PRECONDITION returns WrongExpectedVersion ----

    #[tokio::test]
    async fn three_precondition_failures_returns_wrong_expected_version() {
        let store = MockStore::with_precondition_failures(3);
        let handle = spawn_test_actor(store, "c-fail").await;

        let result = handle
            .execute(CounterCommand::Increment, CommandContext::default())
            .await;

        assert!(
            matches!(result, Err(ExecuteError::WrongExpectedVersion)),
            "expected WrongExpectedVersion, got: {result:?}"
        );
    }

    // ---- Test: full replay from scratch on spawn ----

    #[tokio::test]
    async fn spawn_replays_full_stream_from_scratch() {
        // Seed the mock with 3 Incremented events at versions 0..2.
        let stream_id = stream_uuid("counter", "c-replay");
        let metadata = serde_json::json!({
            "aggregate_type": "counter",
            "instance_id": "c-replay"
        });
        let events: Vec<RecordedEvent> = (0..3)
            .map(|v| RecordedEvent {
                event_id: Uuid::new_v4().to_string(),
                stream_id: stream_id.to_string(),
                stream_version: v,
                global_position: v,
                event_type: "Incremented".to_string(),
                payload: b"null".to_vec(),
                metadata: serde_json::to_vec(&metadata).expect("metadata to vec"),
                recorded_at: 1_700_000_000_000,
            })
            .collect();
        let store = MockStore::new().with_events(events);
        let handle = spawn_test_actor(store, "c-replay").await;

        let state = handle.state().await.expect("state should succeed");
        assert_eq!(
            state.value, 3,
            "state.value should be 3 after replaying 3 Incremented events"
        );
    }

    // ---- Test: empty store produces default state ----

    #[tokio::test]
    async fn spawn_with_empty_store_returns_default_state() {
        let store = MockStore::new();
        let handle = spawn_test_actor(store, "c-empty").await;

        let state = handle.state().await.expect("state should succeed");
        assert_eq!(state.value, 0, "default Counter has value 0");
    }

    // ---- Test: idle timeout shuts down without panicking ----

    #[tokio::test]
    async fn idle_timeout_shuts_down_cleanly() {
        let store = MockStore::new();
        let config = ActorConfig {
            idle_timeout: Duration::from_millis(50),
        };
        let handle = spawn_actor_with_store::<Counter, MockStore>("c-idle", store, config)
            .await
            .expect("spawn should succeed");

        // Wait for the idle timeout to fire.
        tokio::time::sleep(Duration::from_millis(150)).await;

        // The actor should have shut down; the handle is no longer alive.
        assert!(
            !handle.is_alive(),
            "actor should have shut down after idle timeout"
        );
    }
}
