// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::mem::take;

use as_any::AsAny;
use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::Transaction;
use crate::{Result, TableRequirement, TableUpdate};

/// A boxed entry pairing a transaction action with its retry-persistent state.
pub(crate) type TransactionActionEntry = Box<dyn DynActionEntry>;

/// A trait representing an atomic action that can be part of a transaction.
///
/// Implementors of this trait define how a specific action is committed to a table.
/// Each action is responsible for generating the updates and requirements needed
/// to modify the table metadata.
///
/// An action's intent is immutable once applied to a transaction. Retry-persistent
/// state lives in the associated [`TransactionAction::State`], which survives replay
/// attempts of one logical execution but is never shared between executions.
#[async_trait]
pub(crate) trait TransactionAction: Clone + Send + Sync + 'static {
    /// Retry-persistent state exclusively owned by one logical execution of this
    /// action. Stateless actions implement [`StatelessAction`] instead, which
    /// supplies `State = ()`.
    type State: Send + Sync + 'static;

    /// Creates fresh state for one logical execution.
    ///
    /// This is infallible and table-independent; table-dependent initialization
    /// happens during [`TransactionAction::commit`].
    fn new_state(&self) -> Self::State;

    /// Commits this action against the provided table and returns the resulting updates.
    /// NOTE: This function is intended for internal use only and should not be called directly by users.
    ///
    /// One replay attempt: the action (intent) is borrowed immutably, its execution
    /// state mutably, and the table reflects the current transaction-local base.
    ///
    /// # Arguments
    ///
    /// * `state` - The retry-persistent state for this logical execution.
    /// * `table` - The current state of the table this action should apply to.
    ///
    /// # Returns
    ///
    /// An `ActionCommit` containing table updates and table requirements,
    /// or an error if the commit fails.
    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit>;

    /// Best-effort cleanup after the transaction reaches a terminal result.
    ///
    /// Consumes the action and its state: ownership makes any further use
    /// of this action after cleanup a compile error.
    /// Cleanup must not change the already-determined transaction result.
    ///
    /// See [`CommitStatus`] for what each terminal status allows cleanup to do.
    // TODO: invoke this from `Transaction::commit` once terminal status
    // classification is wired up (stateful transaction RFC, milestone 3).
    #[allow(dead_code)]
    async fn cleanup(self, state: Self::State, table: &Table, status: CommitStatus);
}

/// Convenience trait for actions that carry no per-attempt state.
///
/// Implementing `StatelessAction` yields a [`TransactionAction`] impl with
/// `State = ()`, a no-op [`TransactionAction::new_state`], a no-op
/// [`TransactionAction::cleanup`], and a [`TransactionAction::commit`] that
/// forwards to [`StatelessAction::commit`]. Actions that need retry-persistent
/// state (e.g. a persistent snapshot producer) implement `TransactionAction`
/// directly instead.
#[async_trait]
pub(crate) trait StatelessAction: Clone + Send + Sync + 'static {
    /// One commit attempt against the current transaction-local table.
    ///
    /// Same contract as [`TransactionAction::commit`], minus the state parameter.
    async fn commit(&self, table: &Table) -> Result<ActionCommit>;
}

#[async_trait]
impl<T: StatelessAction> TransactionAction for T {
    type State = ();

    fn new_state(&self) {}

    async fn commit(&self, _state: &mut (), table: &Table) -> Result<ActionCommit> {
        StatelessAction::commit(self, table).await
    }

    async fn cleanup(self, _state: (), _table: &Table, _status: CommitStatus) {}
}

/// Classification of a transaction's terminal result, consumed by
/// [`TransactionAction::cleanup`] to determine what is safe to delete.
///
/// The transaction/catalog layer determines this classification; actions
/// consume it rather than independently interpreting catalog errors.
// TODO: produce this classification in `Transaction::commit` once terminal
// cleanup is wired up (stateful transaction RFC, milestone 3).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitStatus {
    /// The catalog confirmed that the commit was applied. Cleanup may remove
    /// owned artifacts not retained by the committed result.
    Committed,
    /// The transaction definitively did not commit and will not retry.
    /// Owned artifacts are deletable.
    Failed,
    /// The commit request was submitted, but its outcome could not be
    /// resolved: the catalog may or may not have applied it. Cleanup must
    /// delete nothing, and the transaction must not be retried.
    ///
    /// Example: every action executed and validated successfully, but the
    /// connection failed while awaiting the catalog's response to
    /// `update_table`.
    Unknown,
}

/// An entry in a transaction, pairing an action's immutable intent with the
/// retry-persistent state of one logical execution.
///
/// The pairing is preserved by construction: the entry is created with fresh
/// state and owns both exclusively, so terminal cleanup can consume them together.
pub(crate) struct ActionEntry<A: TransactionAction> {
    action: A,
    state: A::State,
}

impl<A: TransactionAction> ActionEntry<A> {
    fn new(action: A) -> Self {
        let state = action.new_state();
        Self { action, state }
    }

    /// The action (intent) held by this entry.
    #[allow(dead_code)]
    pub(crate) fn action(&self) -> &A {
        &self.action
    }
}

/// Object-safe adapter over [`ActionEntry`], allowing a transaction to store
/// heterogeneous entries while keeping each action paired with its own state.
#[async_trait]
pub(crate) trait DynActionEntry: AsAny + Send + Sync {
    /// One replay attempt against the current transaction-local table.
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit>;

    /// Best-effort terminal cleanup, consuming the entry.
    // TODO: invoke from `Transaction::commit` once terminal status
    // classification is wired up (stateful transaction RFC, milestone 3).
    #[allow(dead_code)]
    async fn cleanup(self: Box<Self>, table: &Table, status: CommitStatus);

    /// Creates a new entry for a new logical execution of the same action:
    /// the action (intent) is cloned, and it is paired with fresh state from
    /// [`TransactionAction::new_state`]. Accrued retry state is never carried
    /// over to the new entry.
    #[allow(dead_code)]
    fn clone_with_new_state(&self) -> TransactionActionEntry;
}

#[async_trait]
impl<A: TransactionAction> DynActionEntry for ActionEntry<A> {
    async fn commit(&mut self, table: &Table) -> Result<ActionCommit> {
        self.action.commit(&mut self.state, table).await
    }

    async fn cleanup(self: Box<Self>, table: &Table, status: CommitStatus) {
        let ActionEntry { action, state } = *self;
        action.cleanup(state, table, status).await
    }

    fn clone_with_new_state(&self) -> TransactionActionEntry {
        Box::new(ActionEntry::new(self.action.clone()))
    }
}

/// A helper trait for applying a `TransactionAction` to a `Transaction`.
///
/// This is implemented for all `TransactionAction` types
/// to allow easy chaining of actions into a transaction context.
pub trait ApplyTransactionAction {
    /// Adds this action to the given transaction.
    ///
    /// # Arguments
    ///
    /// * `tx` - The transaction to apply the action to.
    ///
    /// # Returns
    ///
    /// The modified transaction containing this action, or an error if the operation fails.
    fn apply(self, tx: Transaction) -> Result<Transaction>;
}

impl<T: TransactionAction> ApplyTransactionAction for T {
    fn apply(self, mut tx: Transaction) -> Result<Transaction>
    where Self: Sized {
        tx.actions.push(Box::new(ActionEntry::new(self)));
        Ok(tx)
    }
}

/// The result of committing a `TransactionAction`.
///
/// This struct contains the updates to apply to the table's metadata
/// and any preconditions that must be satisfied before the update can be committed.
pub struct ActionCommit {
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl ActionCommit {
    /// Creates a new `ActionCommit` from the given updates and requirements.
    pub fn new(updates: Vec<TableUpdate>, requirements: Vec<TableRequirement>) -> Self {
        Self {
            updates,
            requirements,
        }
    }

    /// Consumes and returns the list of table updates.
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }

    /// Consumes and returns the list of table requirements.
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use as_any::Downcast;
    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::Transaction;
    use crate::transaction::action::{
        ActionCommit, ActionEntry, ApplyTransactionAction, CommitStatus, DynActionEntry,
        StatelessAction, TransactionAction,
    };
    use crate::transaction::tests::make_v2_table;
    use crate::{Result, TableRequirement, TableUpdate};

    #[derive(Clone)]
    struct TestAction;

    #[async_trait]
    impl TransactionAction for TestAction {
        type State = ();

        fn new_state(&self) -> Self::State {}

        async fn commit(&self, _state: &mut (), _table: &Table) -> Result<ActionCommit> {
            Ok(ActionCommit::new(
                vec![TableUpdate::SetLocation {
                    location: String::from("s3://bucket/prefix/table/"),
                }],
                vec![TableRequirement::UuidMatch {
                    uuid: Uuid::from_str("9c12d441-03fe-4693-9a96-a0705ddf69c1")?,
                }],
            ))
        }

        async fn cleanup(self, _state: (), _table: &Table, _status: CommitStatus) {}
    }

    #[derive(Clone)]
    struct CountingAction;

    #[async_trait]
    impl TransactionAction for CountingAction {
        type State = u32;

        fn new_state(&self) -> Self::State {
            0
        }

        async fn commit(&self, state: &mut u32, _table: &Table) -> Result<ActionCommit> {
            *state += 1;
            Ok(ActionCommit::new(vec![], vec![]))
        }

        async fn cleanup(self, _state: u32, _table: &Table, _status: CommitStatus) {}
    }

    #[derive(Clone)]
    struct StatelessTestAction;

    #[async_trait]
    impl StatelessAction for StatelessTestAction {
        async fn commit(&self, _table: &Table) -> Result<ActionCommit> {
            Ok(ActionCommit::new(
                vec![TableUpdate::SetLocation {
                    location: String::from("s3://bucket/prefix/stateless/"),
                }],
                vec![],
            ))
        }
    }

    #[tokio::test]
    async fn test_commit_transaction_action() {
        let table = make_v2_table();
        let action = TestAction;

        let mut action_commit = commit_with_fresh_state(action, &table).await.unwrap();

        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert_eq!(updates[0], TableUpdate::SetLocation {
            location: String::from("s3://bucket/prefix/table/")
        });
        assert_eq!(requirements[0], TableRequirement::UuidMatch {
            uuid: Uuid::from_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap()
        });
    }

    #[test]
    fn test_apply_transaction_action() {
        let table = make_v2_table();
        let action = TestAction;
        let tx = Transaction::new(&table);

        let updated_tx = action.apply(tx).unwrap();
        // There should be one action entry in the transaction now
        assert_eq!(updated_tx.actions.len(), 1);

        (*updated_tx.actions[0])
            .downcast_ref::<ActionEntry<TestAction>>()
            .expect("TestAction was not applied to Transaction!");
    }

    #[tokio::test]
    async fn test_entry_retains_state_across_commits() {
        let table = make_v2_table();
        let mut tx = CountingAction.apply(Transaction::new(&table)).unwrap();

        let entry = &mut tx.actions[0];
        entry.commit(&table).await.unwrap();
        entry.commit(&table).await.unwrap();

        let entry = (**entry)
            .downcast_ref::<ActionEntry<CountingAction>>()
            .unwrap();
        assert_eq!(entry.state, 2);
    }

    #[tokio::test]
    async fn test_clone_with_new_state_resets_state() {
        let table = make_v2_table();
        let mut tx = CountingAction.apply(Transaction::new(&table)).unwrap();
        tx.actions[0].commit(&table).await.unwrap();

        let cloned = tx.actions[0].clone_with_new_state();

        let original = (*tx.actions[0])
            .downcast_ref::<ActionEntry<CountingAction>>()
            .unwrap();
        let cloned = (*cloned)
            .downcast_ref::<ActionEntry<CountingAction>>()
            .unwrap();
        assert_eq!(original.state, 1);
        assert_eq!(cloned.state, 0);
    }

    #[tokio::test]
    async fn test_stateless_action_blanket_impl() {
        let table = make_v2_table();

        // commit goes through the blanket TransactionAction impl
        let mut commit = commit_with_fresh_state(StatelessTestAction, &table)
            .await
            .unwrap();
        assert_eq!(commit.take_updates(), vec![TableUpdate::SetLocation {
            location: String::from("s3://bucket/prefix/stateless/"),
        }]);

        // ApplyTransactionAction is available through the blanket impl too
        let tx = StatelessTestAction.apply(Transaction::new(&table)).unwrap();
        assert_eq!(tx.actions.len(), 1);

        // and the erased cleanup forwarding chain compiles and runs
        let entry: Box<ActionEntry<StatelessTestAction>> =
            Box::new(ActionEntry::new(StatelessTestAction));
        entry.cleanup(&table, CommitStatus::Committed).await;
    }

    #[test]
    fn test_action_commit() {
        // Create dummy updates and requirements
        let location = String::from("s3://bucket/prefix/table/");
        let uuid = Uuid::new_v4();
        let updates = vec![TableUpdate::SetLocation { location }];
        let requirements = vec![TableRequirement::UuidMatch { uuid }];

        let mut action_commit = ActionCommit::new(updates.clone(), requirements.clone());

        let taken_updates = action_commit.take_updates();
        let taken_requirements = action_commit.take_requirements();

        // Check values are returned correctly
        assert_eq!(taken_updates, updates);
        assert_eq!(taken_requirements, requirements);

        assert!(action_commit.take_updates().is_empty());
        assert!(action_commit.take_requirements().is_empty());
    }
}
