//! Cross-aggregate workflow coordination (process managers).
//!
//! A process manager reacts to events from the global event log and
//! produces [`CommandEnvelope`]s that are dispatched to (potentially
//! different) aggregates. They are structurally similar to projections
//! -- they use a global cursor for catch-up -- but produce side effects
//! (commands) rather than read models.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use crate::command::CommandEnvelope;
use crate::event::{StoredEvent, decode_stored_event};
use crate::proto::{self, subscribe_response};

/// A cross-aggregate workflow coordinator that reacts to events by
/// producing commands.
///
/// Process managers consume events from all aggregate streams via the
/// gRPC `SubscribeAll` endpoint and emit [`CommandEnvelope`]s for
/// dispatch. Subscription filtering is done in the [`react`](ProcessManager::react)
/// body by inspecting `event.aggregate_type` or `event.event_type`.
///
/// # Contract
///
/// - [`react`](ProcessManager::react) must be deterministic: given the
///   same sequence of events, it must produce the same command envelopes.
/// - Unknown event types or aggregate types should be silently ignored
///   for forward compatibility.
pub trait ProcessManager: Default + Send + Sync + 'static {
    /// Human-readable name, used as an identifier for the process manager.
    const NAME: &'static str;

    /// React to a single event from the global log.
    ///
    /// Returns zero or more [`CommandEnvelope`]s to dispatch. The event
    /// carries `aggregate_type`, `instance_id`, `event_type`, and all
    /// other fields pre-extracted from the gRPC `RecordedEvent`.
    /// Implementors filter on whichever fields they need in the body.
    ///
    /// # Arguments
    ///
    /// * `event` - A reference to the decoded [`StoredEvent`].
    ///
    /// # Returns
    ///
    /// A `Vec` of command envelopes to dispatch. Empty if this event
    /// is irrelevant to the process manager.
    fn react(&mut self, event: &StoredEvent) -> Vec<CommandEnvelope>;
}

// --- ProcessManagerRunner ---

/// Drives a process manager's catch-up loop, reading events from the
/// global log and collecting command envelopes for dispatch.
///
/// Manages the lifecycle of a single [`ProcessManager`]: catching up on
/// new events via `SubscribeAll` and collecting command envelopes. The
/// runner tracks its position in memory (starting from 0 on each startup).
pub(crate) struct ProcessManagerRunner<PM: ProcessManager> {
    /// The process manager's current state.
    state: PM,
    /// Resume token: the next global position to read from.
    last_global_position: u64,
    /// gRPC client for subscribing to the global log.
    client: crate::client::EsClient,
    /// Root directory for this process manager's data files (dead-letter log).
    data_dir: PathBuf,
}

impl<PM: ProcessManager> ProcessManagerRunner<PM> {
    /// Create a new runner starting from global position 0.
    ///
    /// # Arguments
    ///
    /// * `client` - The gRPC client for subscribing to the global event log.
    /// * `data_dir` - Directory for this process manager's data files
    ///   (currently only the dead-letter log).
    pub(crate) fn new(client: crate::client::EsClient, data_dir: PathBuf) -> Self {
        Self {
            state: PM::default(),
            last_global_position: 0,
            client,
            data_dir,
        }
    }

    /// Returns the current process manager state.
    #[allow(dead_code)] // Public API for callers inspecting runner state.
    pub(crate) fn state(&self) -> &PM {
        &self.state
    }

    /// Returns the current global cursor position (resume token).
    #[allow(dead_code)] // Public API for callers inspecting runner state.
    pub(crate) fn position(&self) -> u64 {
        self.last_global_position
    }

    /// Catch up on new events from the global log.
    ///
    /// Subscribes to the global event log starting at the current cursor
    /// position, reads until a `CaughtUp` message, decodes and reacts to
    /// each event, and collects the resulting command envelopes.
    ///
    /// Events with missing or unparseable metadata (non-eventfold-es events)
    /// are silently skipped.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the gRPC subscription fails or the stream
    /// yields an error.
    pub(crate) async fn catch_up(&mut self) -> io::Result<Vec<CommandEnvelope>> {
        let stream = self
            .client
            .subscribe_all_from(self.last_global_position)
            .await
            .map_err(|e| io::Error::other(format!("subscribe_all_from failed: {e}")))?;

        Self::process_stream(&mut self.state, &mut self.last_global_position, stream).await
    }

    /// Process a stream of `SubscribeResponse` messages, updating state
    /// and position.
    ///
    /// Reads from the stream until a `CaughtUp` message is received. For each
    /// `RecordedEvent`, attempts to decode it via [`decode_stored_event`]. Events
    /// that cannot be decoded (missing metadata, invalid UUIDs, etc.) are silently
    /// skipped. Successfully decoded events are passed to
    /// [`ProcessManager::react`] and the resulting envelopes are collected.
    ///
    /// This function is factored out of [`catch_up`] so that tests can provide
    /// a mock stream without needing a live gRPC server.
    async fn process_stream(
        state: &mut PM,
        last_global_position: &mut u64,
        mut stream: impl tokio_stream::Stream<Item = Result<proto::SubscribeResponse, tonic::Status>>
        + Unpin,
    ) -> io::Result<Vec<CommandEnvelope>> {
        tracing::debug!(pm_name = PM::NAME, "starting process manager catch-up");

        let mut envelopes = Vec::new();

        while let Some(result) = stream.next().await {
            let response =
                result.map_err(|e| io::Error::other(format!("subscribe stream error: {e}")))?;

            match response.content {
                Some(subscribe_response::Content::Event(recorded)) => {
                    let produced = react_recorded_event(state, last_global_position, &recorded);
                    envelopes.extend(produced);
                }
                Some(subscribe_response::Content::CaughtUp(_)) => {
                    tracing::debug!("caught up");
                    break;
                }
                None => {
                    // Empty response content; skip.
                }
            }
        }

        Ok(envelopes)
    }

    /// Returns the path to this process manager's dead-letter log.
    pub(crate) fn dead_letter_path(&self) -> PathBuf {
        self.data_dir.join("dead_letters.jsonl")
    }
}

/// Decode a single [`proto::RecordedEvent`] and react to it with a process
/// manager, advancing the position.
///
/// This helper is shared between [`ProcessManagerRunner::process_stream`]
/// (batch catch-up) and [`ProcessManagerCatchUp::react_event`] (single-event
/// live mode). Events that cannot be decoded are silently skipped, but the
/// position always advances.
fn react_recorded_event<PM: ProcessManager>(
    state: &mut PM,
    last_global_position: &mut u64,
    recorded: &proto::RecordedEvent,
) -> Vec<CommandEnvelope> {
    // Skip events already processed. This guards against replay when the live
    // loop subscribes from min_global_position across multiple process managers
    // -- PMs ahead of the minimum would otherwise re-react to events.
    if recorded.global_position < *last_global_position {
        return Vec::new();
    }

    let next_position = recorded.global_position + 1;

    let produced = if let Some(stored) = decode_stored_event(recorded) {
        let envelopes = state.react(&stored);
        tracing::debug!(
            global_position = stored.global_position,
            event_type = %stored.event_type,
            envelopes_produced = envelopes.len(),
            "event reacted"
        );
        envelopes
    } else {
        Vec::new()
    };

    *last_global_position = next_position;
    produced
}

// --- Type-erased trait for store integration ---

/// Trait object interface for process manager runners.
///
/// Allows `run_process_managers` and the live subscription loop to iterate
/// over heterogeneous process managers without knowing each concrete `PM`
/// type. All async methods use boxed futures for trait-object compatibility.
pub(crate) trait ProcessManagerCatchUp: Send + Sync {
    /// Catch up on the global event log and return command envelopes.
    fn catch_up(
        &mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Vec<CommandEnvelope>>> + Send + '_>,
    >;

    /// Decode and react to a single recorded event, advancing the position.
    ///
    /// Used by the live subscription loop to process events one at a time
    /// rather than draining a full stream. Returns any command envelopes
    /// produced by the process manager.
    fn react_event(&mut self, recorded: &proto::RecordedEvent) -> Vec<CommandEnvelope>;

    /// Returns the current global cursor position (resume token).
    fn position(&self) -> u64;

    /// Returns the path to the dead-letter log file.
    fn dead_letter_path(&self) -> PathBuf;
}

impl<PM: ProcessManager> ProcessManagerCatchUp for ProcessManagerRunner<PM> {
    fn catch_up(
        &mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Vec<CommandEnvelope>>> + Send + '_>,
    > {
        Box::pin(self.catch_up())
    }

    fn react_event(&mut self, recorded: &proto::RecordedEvent) -> Vec<CommandEnvelope> {
        react_recorded_event(&mut self.state, &mut self.last_global_position, recorded)
    }

    fn position(&self) -> u64 {
        self.last_global_position
    }

    fn dead_letter_path(&self) -> PathBuf {
        self.dead_letter_path()
    }
}

// --- Dead-letter log ---

/// An entry in the dead-letter log, recording a failed dispatch attempt.
#[derive(Debug, Serialize, Deserialize)]
struct DeadLetterEntry {
    /// The command envelope that failed to dispatch.
    envelope: CommandEnvelope,
    /// Human-readable error message.
    error: String,
    /// Unix timestamp (seconds since epoch) of the failure.
    ts: u64,
}

/// Append a single dead-letter entry to the JSONL log at `path`.
///
/// Creates the file if it does not exist. Each entry is a single JSON line.
///
/// # Errors
///
/// Returns `io::Error` if file I/O fails.
pub(crate) fn append_dead_letter(
    path: &Path,
    envelope: CommandEnvelope,
    error: &str,
) -> io::Result<()> {
    use std::io::Write;
    let ts = std::time::SystemTime::UNIX_EPOCH
        .elapsed()
        .expect("system clock is before Unix epoch")
        .as_secs();
    let entry = DeadLetterEntry {
        envelope,
        error: error.to_string(),
        ts,
    };
    let json = serde_json::to_string(&entry).map_err(io::Error::other)?;
    // Ensure the parent directory exists.
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(file, "{json}")?;
    Ok(())
}

/// Summary of a `run_process_managers` pass.
#[derive(Debug, Clone, Default)]
pub struct ProcessManagerReport {
    /// Number of command envelopes successfully dispatched.
    pub dispatched: usize,
    /// Number of command envelopes written to dead-letter logs.
    pub dead_lettered: usize,
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::*;
    use crate::command::CommandContext;

    /// A test process manager that reacts to counter events by emitting
    /// a command envelope targeting a "target" aggregate.
    #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
    pub(crate) struct EchoSaga {
        /// Number of events processed (for testing state persistence).
        pub events_seen: u64,
    }

    impl ProcessManager for EchoSaga {
        const NAME: &'static str = "echo-saga";

        fn react(&mut self, event: &StoredEvent) -> Vec<CommandEnvelope> {
            if event.aggregate_type != "counter" {
                return Vec::new();
            }
            self.events_seen += 1;
            vec![CommandEnvelope {
                aggregate_type: "target".to_string(),
                instance_id: event.instance_id.clone(),
                command: serde_json::json!({
                    "source_event_type": event.event_type,
                }),
                context: CommandContext::default()
                    .with_correlation_id(format!("echo-{}", self.events_seen)),
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_fixtures::EchoSaga;

    use crate::command::CommandContext;
    use crate::proto::{Empty, RecordedEvent, SubscribeResponse, subscribe_response::Content};

    // --- Helper functions for building mock stream responses ---

    fn make_recorded_event(global_position: u64, stream_version: u64) -> RecordedEvent {
        let event_id = uuid::Uuid::new_v4().to_string();
        let stream_id = crate::event::stream_uuid("counter", "c-1").to_string();
        let metadata = serde_json::json!({
            "aggregate_type": "counter",
            "instance_id": "c-1"
        });
        RecordedEvent {
            event_id,
            stream_id,
            stream_version,
            global_position,
            event_type: "Incremented".to_string(),
            metadata: serde_json::to_vec(&metadata).expect("serialize metadata"),
            payload: b"{}".to_vec(),
            recorded_at: 1_700_000_000_000,
        }
    }

    #[allow(clippy::result_large_err)]
    fn event_response(
        global_position: u64,
        stream_version: u64,
    ) -> Result<SubscribeResponse, tonic::Status> {
        Ok(SubscribeResponse {
            content: Some(Content::Event(make_recorded_event(
                global_position,
                stream_version,
            ))),
        })
    }

    #[allow(clippy::result_large_err)]
    fn caught_up_response() -> Result<SubscribeResponse, tonic::Status> {
        Ok(SubscribeResponse {
            content: Some(Content::CaughtUp(Empty {})),
        })
    }

    // --- Trait shape tests ---

    #[test]
    fn process_manager_trait_has_no_subscriptions_method() {
        let mut saga = EchoSaga::default();
        let stored = StoredEvent {
            event_id: uuid::Uuid::new_v4(),
            stream_id: uuid::Uuid::new_v4(),
            aggregate_type: "counter".to_string(),
            instance_id: "c-1".to_string(),
            stream_version: 0,
            global_position: 0,
            event_type: "Incremented".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
            recorded_at: 1_700_000_000_000,
        };
        let envelopes = saga.react(&stored);
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].aggregate_type, "target");
        assert_eq!(envelopes[0].instance_id, "c-1");
        assert_eq!(envelopes[0].command["source_event_type"], "Incremented");
        assert_eq!(saga.events_seen, 1);
    }

    // --- process_stream / catch_up tests ---

    #[tokio::test]
    async fn catch_up_with_one_valid_event_returns_one_envelope() {
        let mut state = EchoSaga::default();
        let mut position = 0u64;
        let stream = tokio_stream::iter(vec![event_response(0, 0), caught_up_response()]);

        let envelopes =
            ProcessManagerRunner::<EchoSaga>::process_stream(&mut state, &mut position, stream)
                .await
                .expect("process_stream should succeed");

        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].aggregate_type, "target");
        assert_eq!(envelopes[0].instance_id, "c-1");
        assert_eq!(state.events_seen, 1);
        assert_eq!(position, 1);
    }

    #[tokio::test]
    async fn second_catch_up_returns_empty() {
        let mut state = EchoSaga::default();
        let mut position = 0u64;
        let stream = tokio_stream::iter(vec![event_response(0, 0), caught_up_response()]);

        let envelopes =
            ProcessManagerRunner::<EchoSaga>::process_stream(&mut state, &mut position, stream)
                .await
                .expect("first process_stream should succeed");
        assert_eq!(envelopes.len(), 1);

        let stream = tokio_stream::iter(vec![caught_up_response()]);

        let envelopes =
            ProcessManagerRunner::<EchoSaga>::process_stream(&mut state, &mut position, stream)
                .await
                .expect("second process_stream should succeed");
        assert!(envelopes.is_empty());
        assert_eq!(state.events_seen, 1);
        assert_eq!(position, 1);
    }

    #[tokio::test]
    async fn non_es_events_skipped_returns_empty() {
        let recorded = RecordedEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            stream_version: 0,
            global_position: 5,
            event_type: "SomeEvent".to_string(),
            metadata: b"{}".to_vec(),
            payload: b"{}".to_vec(),
            recorded_at: 1_700_000_000_000,
        };
        let stream = tokio_stream::iter(vec![
            Ok(SubscribeResponse {
                content: Some(Content::Event(recorded)),
            }),
            caught_up_response(),
        ]);
        let mut state = EchoSaga::default();
        let mut position = 0u64;

        let envelopes =
            ProcessManagerRunner::<EchoSaga>::process_stream(&mut state, &mut position, stream)
                .await
                .expect("process_stream should succeed");

        assert!(envelopes.is_empty());
        assert_eq!(state.events_seen, 0);
        assert_eq!(position, 6);
    }

    // --- ProcessManagerCatchUp trait method tests ---

    #[tokio::test]
    async fn pm_catch_up_react_event_decodes_and_advances_position() {
        let tmp = tempfile::tempdir().expect("failed to create tmpdir");
        let data_dir = tmp.path().join("echo-saga");

        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = crate::client::EsClient::from_inner(inner);

        let mut runner = ProcessManagerRunner::<EchoSaga>::new(client, data_dir);

        let recorded = make_recorded_event(5, 0);
        let catch_up: &mut dyn ProcessManagerCatchUp = &mut runner;
        let envelopes = catch_up.react_event(&recorded);

        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].aggregate_type, "target");
        assert_eq!(catch_up.position(), 6);
    }

    #[tokio::test]
    async fn pm_catch_up_position_starts_at_zero() {
        let tmp = tempfile::tempdir().expect("failed to create tmpdir");
        let data_dir = tmp.path().join("echo-saga");

        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = crate::client::EsClient::from_inner(inner);

        let runner = ProcessManagerRunner::<EchoSaga>::new(client, data_dir);

        let catch_up: &dyn ProcessManagerCatchUp = &runner;
        assert_eq!(catch_up.position(), 0);
    }

    #[tokio::test]
    async fn pm_catch_up_react_event_skips_non_es_events() {
        let tmp = tempfile::tempdir().expect("failed to create tmpdir");
        let data_dir = tmp.path().join("echo-saga");

        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = crate::client::EsClient::from_inner(inner);

        let mut runner = ProcessManagerRunner::<EchoSaga>::new(client, data_dir);

        let recorded = RecordedEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            stream_id: uuid::Uuid::new_v4().to_string(),
            stream_version: 0,
            global_position: 3,
            event_type: "SomeEvent".to_string(),
            metadata: b"{}".to_vec(),
            payload: b"{}".to_vec(),
            recorded_at: 1_700_000_000_000,
        };

        let catch_up: &mut dyn ProcessManagerCatchUp = &mut runner;
        let envelopes = catch_up.react_event(&recorded);

        assert!(envelopes.is_empty());
        assert_eq!(catch_up.position(), 4);
    }

    // --- Replay guard tests ---

    #[test]
    fn react_recorded_event_skips_already_processed_positions() {
        let mut state = EchoSaga::default();
        let mut position: u64 = 5;

        let old_event = make_recorded_event(3, 3);
        let envelopes = react_recorded_event(&mut state, &mut position, &old_event);
        assert!(
            envelopes.is_empty(),
            "should not react to already-processed event"
        );
        assert_eq!(state.events_seen, 0, "state should not change");
        assert_eq!(position, 5, "position should not change");

        let current_event = make_recorded_event(5, 5);
        let envelopes = react_recorded_event(&mut state, &mut position, &current_event);
        assert_eq!(
            envelopes.len(),
            1,
            "should react to event at current position"
        );
        assert_eq!(state.events_seen, 1);
        assert_eq!(position, 6, "position should advance");

        let future_event = make_recorded_event(10, 10);
        let envelopes = react_recorded_event(&mut state, &mut position, &future_event);
        assert_eq!(
            envelopes.len(),
            1,
            "should react to event ahead of position"
        );
        assert_eq!(state.events_seen, 2);
        assert_eq!(position, 11);
    }

    // --- Dead-letter tests ---

    #[test]
    fn dead_letter_append_creates_readable_jsonl() {
        let tmp = tempfile::tempdir().expect("failed to create tmpdir");
        let path = tmp.path().join("dead_letters.jsonl");

        let envelope = CommandEnvelope {
            aggregate_type: "target".to_string(),
            instance_id: "t-1".to_string(),
            command: serde_json::json!({"action": "test"}),
            context: CommandContext::default(),
        };

        append_dead_letter(&path, envelope, "test error").expect("append should succeed");

        let contents = std::fs::read_to_string(&path).expect("read should succeed");
        let entry: DeadLetterEntry =
            serde_json::from_str(contents.trim()).expect("should be valid JSON");
        assert_eq!(entry.error, "test error");
        assert_eq!(entry.envelope.aggregate_type, "target");
    }
}
