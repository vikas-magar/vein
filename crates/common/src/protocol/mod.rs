//! Inter-node protocol types, vectorized Micro-IR, and RPC definitions.
// Note: `rkyv::Archive` generates internal helper structs for enum variants without field docs,
// which triggers missing_docs warnings even when all user-facing items are fully documented.
#![allow(missing_docs)]

pub mod control;
pub mod convert;
pub mod exchange;
pub mod ir;
pub mod join;
pub mod rpc;
pub mod task;
pub mod vcs_exchange;

// Re-export Micro-IR definitions
pub use ir::constant::ScalarConstant;
pub use ir::op::MicroOp;
pub use ir::program::{MicroProgram, RegId};
pub use ir::state::{AggOpCode, JoinKind, StateMode};
pub use exchange::{
    ExchangeAddressColumn, ExchangeBatch, ExchangeFragment, ExchangeKey, ExchangeManifest, ExchangeState,
};
pub use vcs_exchange::{VcsExchangeRequest, VcsExchangeResponse};
pub use join::{ExchangeInput, JoinKeyMode, JoinKeySpec, JoinSource, PhysicalJoinType};

// Re-export task & pipeline definitions
pub use task::{
    ArchivedExecuteTask, DataSink, DataSource, ExchangeSource, ExecuteTask, ExecutionMode,
    FieldSchema, FileFormat, FileScanTask, KeyValue, ScanPredicate, ScanPredicateValue,
    ShuffleTarget, StorageTarget, StreamSource,
};

// Re-export control-plane events and telemetry
pub use control::{
    ArchivedVeinControlEvent, NodeKind, NodeLoad, QueryTelemetry, TaskTelemetry, VeinControlEvent,
};

// Re-export RPC types
pub use rpc::{
    ArchivedProtocolRequest, ArchivedProtocolResponse, MasterCommand, PreemptResponse,
    ProtocolRequest, ProtocolResponse, VfsRequest,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::{NodeId, QueryId, StageId, TaskId};
    use rkyv::rancor::Error;

    #[test]
    fn test_micro_program_rkyv_zero_copy_roundtrip() {
        let program = MicroProgram::new(
            6,
            vec![
                MicroOp::LoadConstant {
                    dst: 3,
                    value: ScalarConstant::Int64(100),
                },
                MicroOp::Add {
                    dst: 4,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::CmpGt {
                    dst: 5,
                    src1: 4,
                    src2: 3,
                },
                MicroOp::FilterMask { bool_reg: 5 },
                MicroOp::ProjectRegisters {
                    out_regs: vec![0, 4],
                },
            ],
        );

        let bytes = rkyv::to_bytes::<Error>(&program).expect("MicroProgram serialization failed");
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(&bytes);

        let archived = rkyv::access::<ir::program::ArchivedMicroProgram, Error>(&aligned)
            .expect("MicroProgram access failed");

        let roundtrip = MicroProgram::from(archived);
        assert_eq!(roundtrip.register_count, 6);
        assert_eq!(roundtrip.instructions.len(), 5);
        assert_eq!(roundtrip, program);
    }

    #[test]
    fn test_execute_task_rkyv_zero_copy_roundtrip() {
        let task = ExecuteTask {
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::File(FileScanTask {
                files: vec!["data.parquet".to_string()],
                format: FileFormat::Parquet,
                byte_range: None,
                column_ids: vec![0, 1],
                projected_schema: vec![],
                scan_predicates: Vec::new(),
                task_id: TaskId::new(),
                shuffle_target_path: None,
            }),
            program: MicroProgram::new(
                4,
                vec![
                    MicroOp::VectorCosineDistance {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                ],
            ),
            sink: DataSink::Discard,
        };

        let bytes = rkyv::to_bytes::<Error>(&task).expect("ExecuteTask serialization failed");
        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(&bytes);

        let archived = rkyv::access::<task::ArchivedExecuteTask, Error>(&aligned)
            .expect("ExecuteTask access failed");

        let roundtrip = ExecuteTask::from(archived);
        assert_eq!(roundtrip.task_id, task.task_id);
        assert_eq!(roundtrip.query_id, task.query_id);
        assert_eq!(roundtrip.stage_id, task.stage_id);
        assert_eq!(roundtrip.mode, task.mode);
        assert_eq!(roundtrip.program.register_count, 4);
        assert_eq!(roundtrip.program.instructions.len(), 2);
    }

    #[test]
    fn test_control_event_zero_copy_in_place() {
        let task_id = TaskId::new();
        let query_id = QueryId::new();
        let event = VeinControlEvent::CancelTask { task_id, query_id };
        let bytes = rkyv::to_bytes::<Error>(&event).expect("CancelTask serialization failed");

        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(&bytes);

        let archived = rkyv::access::<control::ArchivedVeinControlEvent, Error>(&aligned)
            .expect("CancelTask access failed");

        match archived {
            control::ArchivedVeinControlEvent::CancelTask {
                task_id: archived_tid,
                query_id: archived_qid,
            } => {
                assert_eq!(TaskId::from_raw(archived_tid.0.to_native()), task_id);
                assert_eq!(QueryId::from_raw(archived_qid.0.to_native()), query_id);
            }
            _ => panic!("expected CancelTask"),
        }

        let node_id = NodeId::new();
        let hb = VeinControlEvent::Heartbeat {
            node_id,
            node_kind: NodeKind::Executor,
            timestamp_ms: 999888777,
            load: NodeLoad {
                active_tasks: 3,
                cpu_percent: 25.0,
                memory_bytes: 8192,
            },
            advertise_addr: "127.0.0.1:8081".to_string(),
        };
        let hb_bytes = rkyv::to_bytes::<Error>(&hb).expect("Heartbeat serialization failed");
        let mut hb_aligned = rkyv::util::AlignedVec::<16>::new();
        hb_aligned.extend_from_slice(&hb_bytes);

        let archived_hb =
            rkyv::access::<control::ArchivedVeinControlEvent, Error>(&hb_aligned).expect("Heartbeat access failed");

        match archived_hb {
            control::ArchivedVeinControlEvent::Heartbeat {
                node_id: archived_nid,
                timestamp_ms,
                advertise_addr,
                ..
            } => {
                assert_eq!(NodeId::from_raw(archived_nid.0.to_native()), node_id);
                assert_eq!(timestamp_ms.to_native(), 999888777);
                assert_eq!(advertise_addr.as_str(), "127.0.0.1:8081");
            }
            _ => panic!("expected Heartbeat"),
        }

        let from_archived = VeinControlEvent::from(archived_hb);
        if let VeinControlEvent::Heartbeat {
            node_id: nid,
            advertise_addr: addr,
            ..
        } = from_archived
        {
            assert_eq!(nid, node_id);
            assert_eq!(addr, "127.0.0.1:8081");
        } else {
            panic!("expected Heartbeat");
        }
    }
}
