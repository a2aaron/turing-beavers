use crate::{
    seed::{DecidedNode, MachineDecision, RunStats},
    sql::{Decision, InsertedDecidedRow, InsertedRow, SqlResult, UninsertedPendingRow},
};
use crossbeam::channel::{Receiver, SendError, Sender};
use sqlx::{Connection, SqliteConnection};

pub type WorkUnit = InsertedRow;

pub type SenderProcessorQueue = Sender<WorkerResult>;
pub type ReceiverProcessorQueue = Receiver<WorkerResult>;

pub type SenderWorkerQueue = Sender<WorkUnit>;
pub type ReceiverWorkerQueue = Receiver<WorkUnit>;

pub fn with_starting_queue(machines: Vec<WorkUnit>) -> (ReceiverWorkerQueue, SenderWorkerQueue) {
    let (send, recv) = crossbeam::channel::unbounded();
    for machine in machines {
        send.send(machine).unwrap();
    }
    (recv, send)
}

pub fn add_work_to_queue(
    sender: &SenderWorkerQueue,
    rows: Vec<WorkUnit>,
) -> Result<(), SendError<WorkUnit>> {
    for row in rows {
        sender.send(row)?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Worker result produced by worker threads
pub struct WorkerResult {
    work_unit: WorkUnit,
    decision: MachineDecision,
    stats: RunStats,
}

impl WorkerResult {
    /// Construct a new [WorkerResult] out of an existing [WorkUnit] and [DecidedNode].
    /// The work unit's and node's [MachineTable]s must match or else a panic occurs. The statistics are
    /// taken from the node node.
    pub fn new(work_unit: WorkUnit, node: &DecidedNode) -> WorkerResult {
        assert_eq!(work_unit.machine(), node.machine);
        WorkerResult {
            work_unit,
            decision: node.decision.clone(),
            stats: node.stats,
        }
    }

    /// Submit the results in this [WorkerResult] to the database. This will insert both the decided
    /// row along with its stats as well as its undecided child row if it has any.
    pub async fn submit(
        self,
        conn: &mut SqliteConnection,
    ) -> SqlResult<(InsertedDecidedRow, Vec<WorkUnit>)> {
        let mut txn = conn.begin().await?;

        let decided_row = match self.work_unit {
            InsertedRow::Pending(pending) => {
                let decision = Decision::from(&self.decision);
                pending.update(&mut txn, decision, self.stats).await?
            }
            InsertedRow::Decided(_) => todo!(),
        };

        let pending_rows = if let MachineDecision::EmptyTransition(child_rows) = self.decision {
            let mut out = Vec::with_capacity(child_rows.len());
            for machine in child_rows {
                let pending_row = UninsertedPendingRow { machine };
                let pending_row = pending_row.insert_pending_row(&mut txn).await?;
                out.push(InsertedRow::Pending(pending_row));
            }
            out
        } else {
            vec![]
        };

        txn.commit().await?;
        Ok((decided_row, pending_rows))
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, str::FromStr};

    use smol::block_on;

    use crate::{
        seed::{MachineDecision, PendingNode, BB5_CHAMPION, STARTING_MACHINE},
        sql::{
            create_tables, get_connection, ConnectionMode, Decision, InsertedPendingRow,
            UninsertedPendingRow,
        },
        turing::MachineTable,
        worker::{WorkUnit, WorkerResult},
    };

    #[test]
    fn test_submit_results() {
        block_on(async {
            let mut conn = get_connection(":memory:", ConnectionMode::WriteNew)
                .await
                .unwrap();
            create_tables(&mut conn).await.unwrap();

            let machine = MachineTable::from_str(STARTING_MACHINE).unwrap();
            let decided_node = PendingNode::new(machine).decide();
            let MachineDecision::EmptyTransition(machines) = decided_node.decision.clone() else {
                unreachable!()
            };

            let pending_row = UninsertedPendingRow { machine }
                .insert_pending_row(&mut conn)
                .await
                .unwrap();
            let work_unit = WorkUnit::Pending(pending_row);

            let (inserted_decided_row, inserted_children) =
                WorkerResult::new(work_unit, &decided_node)
                    .submit(&mut conn)
                    .await
                    .unwrap();

            assert_eq!(machines.len(), inserted_children.len());
            assert_eq!(
                inserted_decided_row.decision,
                Decision::from(&decided_node.decision)
            );
            assert_eq!(
                inserted_decided_row.space,
                decided_node.stats.space_used() as u32
            );
            assert_eq!(
                inserted_decided_row.steps,
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

            let (inserted_decided_row, inserted_children) =
                WorkerResult::new(work_unit, &decided_node)
                    .submit(&mut conn)
                    .await
                    .unwrap();

            assert_eq!(0, inserted_children.len());
            assert_eq!(
                inserted_decided_row.decision,
                Decision::from(&decided_node.decision)
            );
            assert_eq!(
                inserted_decided_row.space,
                decided_node.stats.space_used() as u32
            );
            assert_eq!(
                inserted_decided_row.steps,
                decided_node.stats.get_total_steps() as u32
            );

            let actual_pending_queue = InsertedPendingRow::get_pending_rows(&mut conn)
                .await
                .unwrap();

            assert!(actual_pending_queue.is_empty());
        })
    }
}
