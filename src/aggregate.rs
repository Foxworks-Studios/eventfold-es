//! Aggregate trait for domain-driven event sourcing.

use serde::{Serialize, de::DeserializeOwned};

/// A domain aggregate whose state is derived from its event history.
///
/// The implementing type itself serves as the aggregate's state.
/// State is built by folding domain events through the [`apply`](Aggregate::apply) method.
///
/// # Associated Types
///
/// - `Command`: the set of commands this aggregate can handle.
/// - `DomainEvent`: the set of events this aggregate can produce and apply.
/// - `Error`: command rejection / validation error.
///
/// # Contract
///
/// - [`handle`](Aggregate::handle) must be a pure decision function: no I/O, no side effects.
///   It validates a command against the current state and returns zero or more events.
/// - [`apply`](Aggregate::apply) must be a pure, total function. It takes ownership of
///   the current state and a reference to a domain event, returning the next state.
///   Unknown event variants should be ignored for forward compatibility.
pub trait Aggregate: Default + Clone + Send + Sync + 'static {
    /// Identifies this aggregate type (e.g. "order"). Used as a directory name.
    const AGGREGATE_TYPE: &'static str;

    /// The set of commands this aggregate can handle.
    type Command: Send + 'static;

    /// The set of events this aggregate can produce and apply.
    type DomainEvent: Serialize + DeserializeOwned + Send + Sync + Clone + 'static;

    /// Command rejection / validation error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Validate a command against the current state and produce events.
    ///
    /// Returns `Ok(vec![])` if the command is a no-op.
    /// Returns `Err` to reject the command.
    fn handle(&self, cmd: Self::Command) -> Result<Vec<Self::DomainEvent>, Self::Error>;

    /// Apply a single event to produce the next state.
    ///
    /// Unknown event variants should be ignored (return `self` unchanged)
    /// to maintain forward compatibility.
    fn apply(self, event: &Self::DomainEvent) -> Self;
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    use super::Aggregate;
    use serde::{Deserialize, Serialize};

    /// A simple counter aggregate used as a test fixture.
    #[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
    pub(crate) struct Counter {
        pub value: u64,
    }

    /// Commands that can be issued to the `Counter` aggregate.
    #[derive(Clone)]
    pub(crate) enum CounterCommand {
        Increment,
        Decrement,
        Add(u64),
    }

    /// Domain events produced by the `Counter` aggregate.
    ///
    /// Uses adjacently tagged serialization (`"type"` + `"data"`) which is the
    /// convention for all `DomainEvent` types in this crate.
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "type", content = "data")]
    pub(crate) enum CounterEvent {
        Incremented,
        Decremented,
        Added { amount: u64 },
    }

    /// Errors that can occur when handling a `CounterCommand`.
    #[derive(Debug, thiserror::Error)]
    pub(crate) enum CounterError {
        #[error("cannot decrement: counter is already zero")]
        AlreadyZero,
    }

    impl Aggregate for Counter {
        const AGGREGATE_TYPE: &'static str = "counter";

        type Command = CounterCommand;
        type DomainEvent = CounterEvent;
        type Error = CounterError;

        fn handle(&self, cmd: Self::Command) -> Result<Vec<Self::DomainEvent>, Self::Error> {
            match cmd {
                CounterCommand::Increment => Ok(vec![CounterEvent::Incremented]),
                CounterCommand::Decrement => {
                    if self.value == 0 {
                        return Err(CounterError::AlreadyZero);
                    }
                    Ok(vec![CounterEvent::Decremented])
                }
                CounterCommand::Add(n) => Ok(vec![CounterEvent::Added { amount: n }]),
            }
        }

        fn apply(mut self, event: &Self::DomainEvent) -> Self {
            match event {
                CounterEvent::Incremented => self.value += 1,
                CounterEvent::Decremented => self.value -= 1,
                CounterEvent::Added { amount } => self.value += amount,
            }
            self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Aggregate;
    use super::test_fixtures::{Counter, CounterCommand, CounterError, CounterEvent};

    #[test]
    fn handle_increment() {
        let counter = Counter::default();
        let events = counter.handle(CounterCommand::Increment).unwrap();
        assert_eq!(events, vec![CounterEvent::Incremented]);
    }

    #[test]
    fn handle_decrement_nonzero() {
        let counter = Counter { value: 5 };
        let events = counter.handle(CounterCommand::Decrement).unwrap();
        assert_eq!(events, vec![CounterEvent::Decremented]);
    }

    #[test]
    fn handle_decrement_at_zero() {
        let counter = Counter::default();
        let result = counter.handle(CounterCommand::Decrement);
        assert!(result.is_err());
        // Verify the specific error variant via its message.
        let err = result.unwrap_err();
        assert!(
            matches!(err, CounterError::AlreadyZero),
            "expected AlreadyZero, got: {err}"
        );
    }

    #[test]
    fn handle_add() {
        let counter = Counter::default();
        let events = counter.handle(CounterCommand::Add(5)).unwrap();
        assert_eq!(events, vec![CounterEvent::Added { amount: 5 }]);
    }

    #[test]
    fn apply_incremented() {
        let counter = Counter::default().apply(&CounterEvent::Incremented);
        assert_eq!(counter.value, 1);
    }

    #[test]
    fn apply_decremented() {
        let counter = Counter { value: 3 }.apply(&CounterEvent::Decremented);
        assert_eq!(counter.value, 2);
    }

    #[test]
    fn apply_added() {
        let counter = Counter::default().apply(&CounterEvent::Added { amount: 5 });
        assert_eq!(counter.value, 5);
    }

    #[test]
    fn handle_then_apply_roundtrip() {
        let counter = Counter::default();
        let events = counter.handle(CounterCommand::Increment).unwrap();
        // Fold all produced events through `apply` to derive the final state.
        let final_state = events
            .iter()
            .fold(Counter::default(), |state, event| state.apply(event));
        assert_eq!(final_state.value, 1);
    }
}
