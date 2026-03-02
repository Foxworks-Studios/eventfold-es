//! Event-sourcing framework backed by an `eventfold-db` gRPC server.
//!
//! `eventfold-es` provides the building blocks for event-sourced applications:
//! aggregates, projections, process managers, and a typed command bus. All
//! state is persisted via a remote `eventfold-db` server over gRPC.

/// Auto-generated protobuf types for the `eventfold` gRPC service.
pub mod proto {
    tonic::include_proto!("eventfold");
}

mod actor;
pub use actor::AggregateHandle;
mod aggregate;
mod auth;
pub use aggregate::Aggregate;
mod command;
pub use command::{CommandContext, CommandEnvelope};
mod error;
pub use error::{DispatchError, ExecuteError, StateError};
mod event;
pub use event::{
    EventMetadata, ProposedEventData, StoredEvent, decode_stored_event, encode_domain_event,
    stream_uuid,
};
mod client;
pub use client::{EsClient, ExpectedVersionArg};
mod process_manager;
pub use process_manager::{ProcessManager, ProcessManagerReport};
mod live;
pub use live::{LiveConfig, LiveHandle};
mod projection;
mod store;
pub use projection::Projection;
pub use store::{AggregateStore, AggregateStoreBuilder, InjectOptions};

#[cfg(test)]
mod tests {
    use super::proto;

    #[test]
    fn proto_proposed_event_is_accessible() {
        let event = proto::ProposedEvent {
            event_id: "test-id".to_string(),
            event_type: "TestEvent".to_string(),
            metadata: vec![],
            payload: vec![],
        };
        assert_eq!(event.event_id, "test-id");
        assert_eq!(event.event_type, "TestEvent");
    }

    #[test]
    fn proto_recorded_event_is_accessible() {
        let event = proto::RecordedEvent {
            event_id: "evt-1".to_string(),
            stream_id: "stream-1".to_string(),
            stream_version: 0,
            global_position: 42,
            event_type: "Incremented".to_string(),
            metadata: vec![],
            payload: vec![],
            recorded_at: 1_700_000_000_000,
        };
        assert_eq!(event.global_position, 42);
        assert_eq!(event.recorded_at, 1_700_000_000_000);
    }

    #[test]
    fn proto_append_request_is_accessible() {
        let req = proto::AppendRequest {
            stream_id: "stream-1".to_string(),
            expected_version: None,
            events: vec![],
        };
        assert_eq!(req.stream_id, "stream-1");
        assert!(req.events.is_empty());
    }

    #[test]
    fn proto_expected_version_exact_variant() {
        let ev = proto::ExpectedVersion {
            kind: Some(proto::expected_version::Kind::Exact(5)),
        };
        assert!(matches!(
            ev.kind,
            Some(proto::expected_version::Kind::Exact(5))
        ));
    }

    #[test]
    fn proto_subscribe_response_variants() {
        // Verify the oneof variants are accessible.
        let caught_up = proto::SubscribeResponse {
            content: Some(proto::subscribe_response::Content::CaughtUp(
                proto::Empty {},
            )),
        };
        assert!(matches!(
            caught_up.content,
            Some(proto::subscribe_response::Content::CaughtUp(_))
        ));
    }

    #[test]
    fn proto_list_streams_response_is_accessible() {
        let resp = proto::ListStreamsResponse { streams: vec![] };
        assert!(resp.streams.is_empty());
    }

    #[test]
    fn proto_grpc_client_type_exists() {
        // Verify the generated gRPC client type is accessible at compile time.
        // We cannot connect to a server in unit tests, but we can verify
        // the type exists by referencing it in a const assertion.
        fn _assert_client_type_exists<T>(_: T) {}
        // This would fail to compile if the client type doesn't exist.
        let _: fn(proto::event_store_client::EventStoreClient<tonic::transport::Channel>) =
            _assert_client_type_exists;
    }
}
