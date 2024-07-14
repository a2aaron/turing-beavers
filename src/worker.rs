use std::time::Duration;

use crate::{
    seed::{DecidedNode, Decision, RunStats},
    sql::{
        DecisionKind, InsertedDecidedRow, InsertedPendingRow, ResultRowID, RowsAffected,
        SqlQueryResult, SqlResult, UninsertedPendingRow,
    },
    turing::MachineTable,
};
use crossbeam::channel::{Receiver, SendError, Sender};
use sqlx::{Connection, SqliteConnection};

pub type SenderProcessorQueue = Sender<WorkerResult>;
pub type ReceiverProcessorQueue = Receiver<WorkerResult>;

pub type SenderWorkerQueue = Sender<WorkUnit>;
pub type ReceiverWorkerQueue = Receiver<WorkUnit>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftStats {
    pub duration: Duration,
    pub session_id: String,
}

impl SoftStats {
    pub async fn insert(
        &self,
        conn: &mut SqliteConnection,
        results_id: ResultRowID,
    ) -> SqlQueryResult {
        let duration = self.duration.as_secs_f64();
        let result = sqlx::query(
            "INSERT INTO soft_stats(results_id, seconds, session_id, start_time) VALUES($1, $2, $3, datetime('now'))",
        )
        .bind(results_id)
        .bind(duration)
        .bind(&self.session_id)
        .execute(conn)
        .await?;
        assert_eq!(result.rows_affected(), 1);
        Ok(result)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum WorkUnit {
    Pending(InsertedPendingRow),
    Reprocess(InsertedDecidedRow),
}

impl WorkUnit {
    pub fn machine(&self) -> MachineTable {
        match self {
            WorkUnit::Pending(row) => row.machine,
            WorkUnit::Reprocess(row) => row.machine,
        }
    }

    fn results_id(&self) -> i64 {
        match self {
            WorkUnit::Pending(row) => row.id,
            WorkUnit::Reprocess(row) => row.id,
        }
    }
}

pub fn with_starting_queue(machines: Vec<WorkUnit>) -> (ReceiverWorkerQueue, SenderWorkerQueue) {
    let (send, recv) = crossbeam::channel::unbounded();
    for machine in machines {
        send.send(machine).unwrap();
    }
    (recv, send)
}

pub fn add_work_to_queue(
    sender: &SenderWorkerQueue,
    new_work: Vec<WorkUnit>,
) -> Result<(), SendError<WorkUnit>> {
    for work in new_work {
        sender.send(work)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Worker result produced by worker threads
pub struct WorkerResult {
    work_unit: WorkUnit,
    decision: Decision,
    stats: RunStats,
    soft_stats: SoftStats,
}

impl WorkerResult {
    /// Construct a new [WorkerResult] out of an existing [WorkUnit] and [DecidedNode].
    /// The work unit's and node's [MachineTable]s must match or else a panic occurs. The statistics are
    /// taken from the node node.
    pub fn new(work_unit: WorkUnit, node: &DecidedNode, soft_stats: SoftStats) -> WorkerResult {
        assert_eq!(work_unit.machine(), node.machine);
        WorkerResult {
            work_unit,
            decision: node.decision.clone(),
            stats: node.stats,
            soft_stats,
        }
    }

    /// Submit the results in this [WorkerResult] to the database. This will insert both the decided
    /// row along with its stats as well as its undecided child row if it has any.
    pub async fn submit(
        self,
        conn: &mut SqliteConnection,
    ) -> SqlResult<(RowsAffected, InsertedDecidedRow, Vec<WorkUnit>)> {
        let mut txn = conn.begin().await?;
        let mut rows_affected = 0;
        let (decided_row, new_work_units) = match self.work_unit {
            WorkUnit::Pending(pending) => {
                let decision = DecisionKind::from(&self.decision);

                let (decided_row, this_rows_affected) =
                    pending.update(&mut txn, decision, self.stats).await?;
                rows_affected += this_rows_affected;

                let pending_rows = if let Decision::EmptyTransition(child_rows) = self.decision {
                    let mut out = Vec::with_capacity(child_rows.len());
                    for machine in child_rows {
                        let pending_row = UninsertedPendingRow { machine };
                        let pending_row = pending_row.insert_pending_row(&mut txn).await?;
                        rows_affected += 1;
                        out.push(WorkUnit::Pending(pending_row));
                    }
                    out
                } else {
                    vec![]
                };

                (decided_row, pending_rows)
            }
            WorkUnit::Reprocess(decided_row) => {
                // Check that the decision we found for this row matches the existing one.
                // If not, panic, since this should never happen.
                // We don't do anything for this row since there isn't actually anything to update.
                assert_eq!(decided_row.machine, self.work_unit.machine());
                assert_eq!(decided_row.decision, DecisionKind::from(&self.decision));
                assert_eq!(decided_row.stats.steps, self.stats.get_total_steps() as u32);
                assert_eq!(decided_row.stats.space, self.stats.space_used() as u32);
                // Do not return any WorkUnits from submitting this WorkUnit
                // We don't want any additional rows to be inserted anyways since this is a reprocess
                // Since the reprocess mode already fetches all decided rows, this means any child work units
                // that would normally be generated are either already pending (in which case, we don't care
                // about reprocessing them) or are already decided (in which case they were already picked up)
                (decided_row, vec![])
            }
        };

        let result = self
            .soft_stats
            .insert(&mut txn, self.work_unit.results_id())
            .await?;
        rows_affected += result.rows_affected();

        txn.commit().await?;
        Ok((rows_affected, decided_row, new_work_units))
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, str::FromStr, time::Duration};

    use smol::block_on;

    use crate::{
        seed::{Decision, PendingNode, BB5_CHAMPION, STARTING_MACHINE},
        sql::{
            create_tables, get_connection, ConnectionMode, DecisionKind, InsertedPendingRow,
            UninsertedPendingRow,
        },
        turing::MachineTable,
        worker::{SoftStats, WorkUnit, WorkerResult},
    };

    impl SoftStats {
        /// Create a blank SoftStats struct. Useful for tests for when you don't actually care about
        /// the value of this struct.
        fn blank() -> SoftStats {
            SoftStats {
                duration: Duration::from_secs(1),
                session_id: "none".to_string(),
            }
        }
    }

    #[test]
    fn test_submit_results() {
        block_on(async {
            let mut conn = get_connection(":memory:", ConnectionMode::WriteNew)
                .await
                .unwrap();
            create_tables(&mut conn).await.unwrap();

            let machine = MachineTable::from_str(STARTING_MACHINE).unwrap();
            let decided_node = PendingNode::new(machine).decide();
            let Decision::EmptyTransition(machines) = decided_node.decision.clone() else {
                unreachable!()
            };

            let pending_row = UninsertedPendingRow { machine }
                .insert_pending_row(&mut conn)
                .await
                .unwrap();
            let work_unit = WorkUnit::Pending(pending_row);

            let (_, inserted_decided_row, inserted_children) =
                WorkerResult::new(work_unit, &decided_node, SoftStats::blank())
                    .submit(&mut conn)
                    .await
                    .unwrap();

            assert_eq!(machines.len(), inserted_children.len());
            assert_eq!(
                inserted_decided_row.decision,
                DecisionKind::from(&decided_node.decision)
            );
            assert_eq!(
                inserted_decided_row.stats.space,
                decided_node.stats.space_used() as u32
            );
            assert_eq!(
                inserted_decided_row.stats.steps,
                decided_node.stats.get_total_steps() as u32
            );

            let expected_pending_queue: HashSet<MachineTable> = machines.into_iter().collect();
            let actual_pending_queue: HashSet<MachineTable> =
                InsertedPendingRow::get_pending_rows(&mut conn)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|x| x.machine)
                    .collect();

            assert_eq!(expected_pending_queue, actual_pending_queue);
        })
    }

    #[test]
    fn test_submit_results2() {
        block_on(async {
            let mut conn = get_connection(":memory:", ConnectionMode::WriteNew)
                .await
                .unwrap();
            create_tables(&mut conn).await.unwrap();

            let machine = MachineTable::from_str(BB5_CHAMPION).unwrap();
            let decided_node = PendingNode::new(machine).decide();

            let pending_row = UninsertedPendingRow { machine }
                .insert_pending_row(&mut conn)
                .await
                .unwrap();
            let work_unit = WorkUnit::Pending(pending_row);

            let (_, inserted_decided_row, inserted_children) =
                WorkerResult::new(work_unit, &decided_node, SoftStats::blank())
                    .submit(&mut conn)
                    .await
                    .unwrap();

            assert_eq!(0, inserted_children.len());
            assert_eq!(
                inserted_decided_row.decision,
                DecisionKind::from(&decided_node.decision)
            );
            assert_eq!(
                inserted_decided_row.stats.space,
                decided_node.stats.space_used() as u32
            );
            assert_eq!(
                inserted_decided_row.stats.steps,
                decided_node.stats.get_total_steps() as u32
            );

            let actual_pending_queue = InsertedPendingRow::get_pending_rows(&mut conn)
                .await
                .unwrap();

            assert!(actual_pending_queue.is_empty());
        })
    }
}
