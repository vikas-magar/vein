//! Inter-node protocol types, vectorized Micro-IR, and RPC definitions.
// Note: `rkyv::Archive` generates internal helper structs for enum variants without field docs,
// which triggers missing_docs warnings even when all user-facing items are fully documented.
#![allow(missing_docs)]

pub mod convert;
pub mod ir;
pub mod task;

// Re-export Micro-IR definitions
pub use ir::constant::ScalarConstant;
pub use ir::op::MicroOp;
pub use ir::program::{MicroProgram, RegId};
pub use ir::state::{AggOpCode, JoinKind, StateMode};

// Re-export task & pipeline definitions
pub use task::{
    ArchivedCatalogSnapshotToken, ArchivedExecuteTask, CatalogSnapshotToken, DataSink, DataSource,
    ExecuteTask, ExecutionMode, FieldSchema, FileFormat, FileScanTask, KeyValue, StorageTarget,
    StreamSource,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identifiers::{QueryId, RequestId, StageId, TaskId};
    use rkyv::rancor::Error;
    use std::sync::Arc;

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
            request_id: RequestId::new(),
            mode: ExecutionMode::Batch,
            task_id: TaskId::new(),
            attempt: 0,
            query_id: QueryId::new(),
            stage_id: StageId::new(),
            source: DataSource::File(FileScanTask {
                files: vec!["data.parquet".to_string()],
                format: FileFormat::Parquet,
                byte_range: None,
                column_ids: vec![0, 1],
                projected_schema: vec![],
                task_id: TaskId::new(),
                catalog_snapshot: None,
            }),
            program: Arc::new(MicroProgram::new(
                4,
                vec![
                    MicroOp::VectorCosineDistance {
                        dst: 2,
                        src1: 0,
                        src2: 1,
                    },
                    MicroOp::FilterMask { bool_reg: 2 },
                ],
            )),
            sink: Arc::new(DataSink::Discard),
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
}
