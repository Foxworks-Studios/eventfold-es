//! Cross-stream projections (read models) backed by the global event log.
//!
//! Projections consume events from all aggregate streams via the gRPC
//! `SubscribeAll` endpoint. Each projection maintains a single global
//! cursor position instead of per-stream byte offsets.

use std::any::Any;
use std::future::Future;
use std::io;
use std::pin::Pin;

use tokio_stream::StreamExt;

use crate::event::{StoredEvent, decode_stored_event};
use crate::proto::{self, subscribe_response};

/// A cross-stream read model that consumes events from the global log.
///
/// Projections are eventually consistent: they catch up by reading new
/// events from the global log via `SubscribeAll` and are rebuilt from
/// scratch on every startup.
///
/// # Contract
///
/// - [`apply`](Projection::apply) must be deterministic: given the same
///   sequence of events, it must produce the same state.
/// - Unknown event types or aggregate types should be silently ignored
///   for forward compatibility. Filtering by `event.aggregate_type` or
///   `event.event_type` is done in the method body.
pub trait Projection: Default + Clone + Send + Sync + 'static {
    /// Human-readable name, used as an identifier for the projection.
    const NAME: &'static str;

    /// Apply a single event from the global log.
    ///
    /// The event carries `aggregate_type`, `instance_id`, `event_type`,
    /// and all other fields pre-extracted from the gRPC `RecordedEvent`.
    /// Implementors filter on whichever fields they need in the body.
    fn apply(&mut self, event: &StoredEvent);
}

/// Drives a projection's catch-up loop, reading events from the global log.
///
/// Manages the lifecycle of a single [`Projection`]: catching up on new
/// events via `SubscribeAll` and maintaining an in-memory cursor position.
pub(crate) struct ProjectionRunner<P: Projection> {
    /// The projection's current state.
    state: P,
    /// Resume token: the next global position to read from.
    ///
    /// After processing an event at global position N, this is set to N + 1.
    /// A value of 0 means no events have been processed yet.
    last_global_position: u64,
    /// gRPC client for subscribing to the global log.
    client: crate::client::EsClient,
}

impl<P: Projection> ProjectionRunner<P> {
    /// Create a new runner starting from global position 0.
    ///
    /// # Arguments
    ///
    /// * `client` - The gRPC client for subscribing to the global event log.
    pub(crate) fn new(client: crate::client::EsClient) -> Self {
        Self {
            state: P::default(),
            last_global_position: 0,
            client,
        }
    }

    /// Returns the current projection state.
    #[allow(dead_code)] // Superseded by ProjectionCatchUp::state_any for store access.
    pub(crate) fn state(&self) -> &P {
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
    /// position and reads until a `CaughtUp` message.
    ///
    /// Events with missing or unparseable metadata (non-eventfold-es events)
    /// are silently skipped.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the gRPC subscription fails or the stream
    /// yields an error.
    pub(crate) async fn catch_up(&mut self) -> io::Result<()> {
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
    /// skipped. Successfully decoded events are applied to the projection.
    ///
    /// This function is factored out of [`catch_up`] so that tests can provide
    /// a mock stream without needing a live gRPC server.
    async fn process_stream(
        state: &mut P,
        last_global_position: &mut u64,
        mut stream: impl tokio_stream::Stream<Item = Result<proto::SubscribeResponse, tonic::Status>>
        + Unpin,
    ) -> io::Result<()> {
        tracing::debug!(projection_name = P::NAME, "starting projection catch-up");

        while let Some(result) = stream.next().await {
            let response =
                result.map_err(|e| io::Error::other(format!("subscribe stream error: {e}")))?;

            match response.content {
                Some(subscribe_response::Content::Event(recorded)) => {
                    apply_recorded_event(state, last_global_position, &recorded);
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

        Ok(())
    }
}

/// Decode a single [`proto::RecordedEvent`] and apply it to a projection's state,
/// advancing the position.
///
/// This helper is shared between [`ProjectionRunner::process_stream`] (batch
/// catch-up) and [`ProjectionCatchUp::apply_event`] (single-event live mode).
/// Events that cannot be decoded (missing metadata, invalid UUIDs, etc.) are
/// silently skipped, but the position always advances.
fn apply_recorded_event<P: Projection>(
    state: &mut P,
    last_global_position: &mut u64,
    recorded: &proto::RecordedEvent,
) {
    // Skip events already processed. This guards against replay when the live
    // loop subscribes from min_global_position across multiple projections --
    // projections ahead of the minimum would otherwise re-apply events.
    if recorded.global_position < *last_global_position {
        return;
    }

    let next_position = recorded.global_position + 1;

    if let Some(stored) = decode_stored_event(recorded) {
        state.apply(&stored);
        tracing::debug!(
            global_position = stored.global_position,
            event_type = %stored.event_type,
            "event applied"
        );
    }

    *last_global_position = next_position;
}

// --- Type-erased trait for store integration ---

/// Type-erased interface for projection runners.
///
/// Allows the live subscription loop and `AggregateStore` to interact with
/// heterogeneous projections without knowing each concrete `P` type. All
/// async methods use boxed futures for trait-object compatibility.
pub(crate) trait ProjectionCatchUp: Send + Sync {
    /// Catch up on the global event log by subscribing from the current
    /// position.
    fn catch_up(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;

    /// Decode and apply a single recorded event, advancing the position.
    ///
    /// Used by the live subscription loop to process events one at a time
    /// rather than draining a full stream.
    fn apply_event(&mut self, recorded: &proto::RecordedEvent);

    /// Returns the current global cursor position (resume token).
    fn position(&self) -> u64;

    /// Clone the current projection state into a type-erased box.
    ///
    /// The caller can downcast the returned `Box<dyn Any>` to the concrete
    /// projection type `P` to read the state.
    fn state_any(&self) -> Box<dyn Any + Send>;
}

impl<P: Projection> ProjectionCatchUp for ProjectionRunner<P> {
    fn catch_up(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(self.catch_up())
    }

    fn apply_event(&mut self, recorded: &proto::RecordedEvent) {
        apply_recorded_event(&mut self.state, &mut self.last_global_position, recorded);
    }

    fn position(&self) -> u64 {
        self.last_global_position
    }

    fn state_any(&self) -> Box<dyn Any + Send> {
        Box::new(self.state.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Empty, RecordedEvent, SubscribeResponse, subscribe_response::Content};

    /// A test projection that counts all events.
    #[derive(Debug, Clone, Default, PartialEq)]
    struct EventCounter {
        pub count: u64,
    }

    impl Projection for EventCounter {
        const NAME: &'static str = "event-counter";

        fn apply(&mut self, _event: &StoredEvent) {
            self.count += 1;
        }
    }

    /// Build a `RecordedEvent` with valid eventfold-es metadata.
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

    /// Build a `SubscribeResponse` wrapping a `RecordedEvent`.
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

    /// Build a `SubscribeResponse` with the `CaughtUp` sentinel.
    #[allow(clippy::result_large_err)]
    fn caught_up_response() -> Result<SubscribeResponse, tonic::Status> {
        Ok(SubscribeResponse {
            content: Some(Content::CaughtUp(Empty {})),
        })
    }

    #[test]
    fn projection_trait_has_no_subscriptions_method() {
        let mut counter = EventCounter::default();
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
        counter.apply(&stored);
        assert_eq!(counter.count, 1);
    }

    #[tokio::test]
    async fn catch_up_fresh_with_two_events() {
        let mut state = EventCounter::default();
        let mut position = 0u64;
        let stream = tokio_stream::iter(vec![
            event_response(0, 0),
            event_response(1, 1),
            caught_up_response(),
        ]);

        ProjectionRunner::<EventCounter>::process_stream(&mut state, &mut position, stream)
            .await
            .expect("process_stream should succeed");

        assert_eq!(state.count, 2);
        assert_eq!(position, 2);
    }

    #[tokio::test]
    async fn second_catch_up_with_only_caught_up_leaves_count_unchanged() {
        let mut state = EventCounter { count: 2 };
        let mut position = 2u64;
        let stream = tokio_stream::iter(vec![caught_up_response()]);

        ProjectionRunner::<EventCounter>::process_stream(&mut state, &mut position, stream)
            .await
            .expect("process_stream should succeed");

        assert_eq!(state.count, 2);
        assert_eq!(position, 2);
    }

    #[tokio::test]
    async fn recorded_event_with_empty_metadata_is_skipped() {
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
        let mut state = EventCounter::default();
        let mut position = 0u64;

        ProjectionRunner::<EventCounter>::process_stream(&mut state, &mut position, stream)
            .await
            .expect("process_stream should succeed");

        assert_eq!(state.count, 0);
        assert_eq!(position, 6);
    }

    #[tokio::test]
    async fn apply_event_decodes_and_advances_position() {
        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = crate::client::EsClient::from_inner(inner);

        let mut runner = ProjectionRunner::<EventCounter>::new(client);

        let recorded = make_recorded_event(5, 0);
        let catch_up: &mut dyn ProjectionCatchUp = &mut runner;
        catch_up.apply_event(&recorded);

        assert_eq!(catch_up.position(), 6);
        let state_box = catch_up.state_any();
        let state = state_box
            .downcast::<EventCounter>()
            .expect("downcast should succeed");
        assert_eq!(state.count, 1);
    }

    #[tokio::test]
    async fn position_starts_at_zero() {
        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = crate::client::EsClient::from_inner(inner);

        let runner = ProjectionRunner::<EventCounter>::new(client);

        let catch_up: &dyn ProjectionCatchUp = &runner;
        assert_eq!(catch_up.position(), 0);
    }

    #[tokio::test]
    async fn state_any_returns_cloned_state() {
        let channel = tonic::transport::Endpoint::from_static("http://[::1]:1").connect_lazy();
        let inner = crate::proto::event_store_client::EventStoreClient::new(channel);
        let client = crate::client::EsClient::from_inner(inner);

        let mut runner = ProjectionRunner::<EventCounter>::new(client);

        let recorded = make_recorded_event(0, 0);
        let catch_up: &mut dyn ProjectionCatchUp = &mut runner;
        catch_up.apply_event(&recorded);
        let recorded = make_recorded_event(1, 1);
        catch_up.apply_event(&recorded);

        let state_box = catch_up.state_any();
        let state = state_box
            .downcast::<EventCounter>()
            .expect("downcast should succeed");
        assert_eq!(state.count, 2);
    }

    #[test]
    fn apply_recorded_event_skips_already_processed_positions() {
        let mut state = EventCounter::default();
        let mut position: u64 = 5;

        let old_event = make_recorded_event(3, 3);
        apply_recorded_event(&mut state, &mut position, &old_event);
        assert_eq!(state.count, 0, "should not apply already-processed event");
        assert_eq!(position, 5, "position should not change");

        let current_event = make_recorded_event(5, 5);
        apply_recorded_event(&mut state, &mut position, &current_event);
        assert_eq!(state.count, 1, "should apply event at current position");
        assert_eq!(position, 6, "position should advance");

        let future_event = make_recorded_event(10, 10);
        apply_recorded_event(&mut state, &mut position, &future_event);
        assert_eq!(state.count, 2, "should apply event ahead of position");
        assert_eq!(position, 11);
    }
}
