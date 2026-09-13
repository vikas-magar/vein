//! Zero-copy From<&Archived*> conversions for all protocol wire types.
//!
//! Provides fast O(1) in-place extraction from validated rkyv buffers without
//! lifetime infection or rkyv::Deserialize Pool strategy mismatches.

use crate::identifiers::{NodeId, QueryId, StageId, TaskId};
use super::control::{
    ArchivedNodeKind, ArchivedNodeLoad, ArchivedVeinControlEvent, NodeKind, NodeLoad,
    VeinControlEvent,
};
use super::ir::constant::{ArchivedScalarConstant, ScalarConstant};
use super::ir::op::{ArchivedMicroOp, MicroOp};
use super::ir::program::{ArchivedMicroProgram, MicroProgram};
use super::ir::state::{
    ArchivedAggOpCode, ArchivedJoinKind, ArchivedStateMode, AggOpCode, JoinKind, StateMode,
};
use super::task::{
    ArchivedDataSink, ArchivedDataSource, ArchivedExecuteTask, ArchivedExecutionMode,
    ArchivedFieldSchema, ArchivedFileFormat, ArchivedFileScanTask, DataSink, DataSource,
    ExchangeSource, ExecuteTask, ExecutionMode, FieldSchema, FileFormat, FileScanTask,
    ShuffleTarget, StorageTarget, StreamSource,
};

// ── Micro-IR Conversions ──────────────────────────────────────────

impl From<&ArchivedScalarConstant> for ScalarConstant {
    fn from(archived: &ArchivedScalarConstant) -> Self {
        match archived {
            ArchivedScalarConstant::Null => ScalarConstant::Null,
            ArchivedScalarConstant::Boolean(b) => ScalarConstant::Boolean(*b),
            ArchivedScalarConstant::Int8(i) => ScalarConstant::Int8(*i),
            ArchivedScalarConstant::Int16(i) => ScalarConstant::Int16(i.to_native()),
            ArchivedScalarConstant::Int32(i) => ScalarConstant::Int32(i.to_native()),
            ArchivedScalarConstant::Int64(i) => ScalarConstant::Int64(i.to_native()),
            ArchivedScalarConstant::UInt8(u) => ScalarConstant::UInt8(*u),
            ArchivedScalarConstant::UInt16(u) => ScalarConstant::UInt16(u.to_native()),
            ArchivedScalarConstant::UInt32(u) => ScalarConstant::UInt32(u.to_native()),
            ArchivedScalarConstant::UInt64(u) => ScalarConstant::UInt64(u.to_native()),
            ArchivedScalarConstant::Float32(f) => ScalarConstant::Float32(f.to_native()),
            ArchivedScalarConstant::Float64(f) => ScalarConstant::Float64(f.to_native()),
            ArchivedScalarConstant::Utf8(s) => ScalarConstant::Utf8(s.as_str().to_string()),
            ArchivedScalarConstant::Binary(b) => ScalarConstant::Binary(b.as_slice().to_vec()),
            ArchivedScalarConstant::Vector(v) => {
                ScalarConstant::Vector(v.iter().map(|f| f.to_native()).collect())
            }
        }
    }
}

impl From<&ArchivedStateMode> for StateMode {
    fn from(archived: &ArchivedStateMode) -> Self {
        match archived {
            ArchivedStateMode::HashJoinTable => StateMode::HashJoinTable,
            ArchivedStateMode::TwoPhaseAggregatePartial { num_partitions } => {
                StateMode::TwoPhaseAggregatePartial {
                    num_partitions: num_partitions.to_native(),
                }
            }
            ArchivedStateMode::TwoPhaseAggregateMerge => StateMode::TwoPhaseAggregateMerge,
            ArchivedStateMode::WindowBuffer => StateMode::WindowBuffer,
            ArchivedStateMode::SortBuffer => StateMode::SortBuffer,
            ArchivedStateMode::TopNHeap { n } => StateMode::TopNHeap { n: n.to_native() },
        }
    }
}

impl From<&ArchivedJoinKind> for JoinKind {
    fn from(archived: &ArchivedJoinKind) -> Self {
        match archived {
            ArchivedJoinKind::Inner => JoinKind::Inner,
            ArchivedJoinKind::Left => JoinKind::Left,
            ArchivedJoinKind::Right => JoinKind::Right,
            ArchivedJoinKind::Full => JoinKind::Full,
            ArchivedJoinKind::Semi => JoinKind::Semi,
            ArchivedJoinKind::Anti => JoinKind::Anti,
        }
    }
}

impl From<&ArchivedAggOpCode> for AggOpCode {
    fn from(archived: &ArchivedAggOpCode) -> Self {
        match archived {
            ArchivedAggOpCode::Count => AggOpCode::Count,
            ArchivedAggOpCode::Sum => AggOpCode::Sum,
            ArchivedAggOpCode::Min => AggOpCode::Min,
            ArchivedAggOpCode::Max => AggOpCode::Max,
            ArchivedAggOpCode::Avg => AggOpCode::Avg,
        }
    }
}

impl From<&ArchivedMicroOp> for MicroOp {
    fn from(archived: &ArchivedMicroOp) -> Self {
        match archived {
            ArchivedMicroOp::LoadConstant { dst, value } => MicroOp::LoadConstant {
                dst: *dst,
                value: ScalarConstant::from(value),
            },
            ArchivedMicroOp::CopyRegister { dst, src } => MicroOp::CopyRegister {
                dst: *dst,
                src: *src,
            },
            ArchivedMicroOp::Add { dst, src1, src2 } => MicroOp::Add {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Sub { dst, src1, src2 } => MicroOp::Sub {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Mul { dst, src1, src2 } => MicroOp::Mul {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Div { dst, src1, src2 } => MicroOp::Div {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Mod { dst, src1, src2 } => MicroOp::Mod {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Neg { dst, src } => MicroOp::Neg {
                dst: *dst,
                src: *src,
            },
            ArchivedMicroOp::CmpEq { dst, src1, src2 } => MicroOp::CmpEq {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::CmpNeq { dst, src1, src2 } => MicroOp::CmpNeq {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::CmpLt { dst, src1, src2 } => MicroOp::CmpLt {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::CmpLte { dst, src1, src2 } => MicroOp::CmpLte {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::CmpGt { dst, src1, src2 } => MicroOp::CmpGt {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::CmpGte { dst, src1, src2 } => MicroOp::CmpGte {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::And { dst, src1, src2 } => MicroOp::And {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Or { dst, src1, src2 } => MicroOp::Or {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::Not { dst, src } => MicroOp::Not {
                dst: *dst,
                src: *src,
            },
            ArchivedMicroOp::IsNull { dst, src } => MicroOp::IsNull {
                dst: *dst,
                src: *src,
            },
            ArchivedMicroOp::IsNotNull { dst, src } => MicroOp::IsNotNull {
                dst: *dst,
                src: *src,
            },
            ArchivedMicroOp::VectorCosineDistance { dst, src1, src2 } => {
                MicroOp::VectorCosineDistance {
                    dst: *dst,
                    src1: *src1,
                    src2: *src2,
                }
            }
            ArchivedMicroOp::VectorL2Distance { dst, src1, src2 } => MicroOp::VectorL2Distance {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::VectorDotProduct { dst, src1, src2 } => MicroOp::VectorDotProduct {
                dst: *dst,
                src1: *src1,
                src2: *src2,
            },
            ArchivedMicroOp::CallUdf {
                name,
                arg_regs,
                dst,
            } => MicroOp::CallUdf {
                name: name.as_str().to_string(),
                arg_regs: arg_regs.iter().copied().collect(),
                dst: *dst,
            },
            ArchivedMicroOp::FilterMask { bool_reg } => MicroOp::FilterMask {
                bool_reg: *bool_reg,
            },
            ArchivedMicroOp::ProjectRegisters { out_regs } => MicroOp::ProjectRegisters {
                out_regs: out_regs.iter().copied().collect(),
            },
            ArchivedMicroOp::StateBuild {
                state_id,
                key_regs,
                payload_regs,
                mode,
            } => MicroOp::StateBuild {
                state_id: state_id.to_native(),
                key_regs: key_regs.iter().copied().collect(),
                payload_regs: payload_regs.iter().copied().collect(),
                mode: StateMode::from(mode),
            },
            ArchivedMicroOp::StateProbe {
                state_id,
                key_regs,
                out_regs,
                join_kind,
            } => MicroOp::StateProbe {
                state_id: state_id.to_native(),
                key_regs: key_regs.iter().copied().collect(),
                out_regs: out_regs.iter().copied().collect(),
                join_kind: JoinKind::from(join_kind),
            },
            ArchivedMicroOp::StateFold {
                state_id,
                agg_regs,
                op,
            } => MicroOp::StateFold {
                state_id: state_id.to_native(),
                agg_regs: agg_regs.iter().copied().collect(),
                op: AggOpCode::from(op),
            },
        }
    }
}

impl From<&ArchivedMicroProgram> for MicroProgram {
    fn from(archived: &ArchivedMicroProgram) -> Self {
        Self {
            register_count: archived.register_count,
            instructions: archived.instructions.iter().map(MicroOp::from).collect(),
        }
    }
}

// ── Task & Plan Conversions ───────────────────────────────────────

impl From<&ArchivedFileFormat> for FileFormat {
    fn from(archived: &ArchivedFileFormat) -> Self {
        match archived {
            ArchivedFileFormat::Parquet => FileFormat::Parquet,
            ArchivedFileFormat::Csv => FileFormat::Csv,
            ArchivedFileFormat::Json => FileFormat::Json,
        }
    }
}

impl From<&ArchivedFieldSchema> for FieldSchema {
    fn from(archived: &ArchivedFieldSchema) -> Self {
        Self {
            field_id: archived.field_id.to_native(),
            name: archived.name.as_str().to_string(),
            data_type: archived.data_type.as_str().to_string(),
        }
    }
}

impl From<&ArchivedFileScanTask> for FileScanTask {
    fn from(archived: &ArchivedFileScanTask) -> Self {
        Self {
            files: archived
                .files
                .iter()
                .map(|f| f.as_str().to_string())
                .collect(),
            format: FileFormat::from(&archived.format),
            byte_range: archived
                .byte_range
                .as_ref()
                .map(|r| r.start.to_native()..r.end.to_native()),
            column_ids: archived.column_ids.iter().map(|c| c.to_native()).collect(),
            projected_schema: archived
                .projected_schema
                .iter()
                .map(FieldSchema::from)
                .collect(),
            task_id: TaskId::from_raw(archived.task_id.0.to_native()),
            shuffle_target_path: archived
                .shuffle_target_path
                .as_ref()
                .map(|p| p.as_str().to_string()),
        }
    }
}

impl From<&ArchivedExecutionMode> for ExecutionMode {
    fn from(archived: &ArchivedExecutionMode) -> Self {
        match archived {
            ArchivedExecutionMode::Batch => ExecutionMode::Batch,
            ArchivedExecutionMode::Streaming => ExecutionMode::Streaming,
        }
    }
}

impl From<&ArchivedDataSource> for DataSource {
    fn from(archived: &ArchivedDataSource) -> Self {
        match archived {
            ArchivedDataSource::Memory => DataSource::Memory,
            ArchivedDataSource::File(scan) => DataSource::File(FileScanTask::from(scan)),
            ArchivedDataSource::Exchange(e) => DataSource::Exchange(ExchangeSource {
                upstream_stage_id: StageId::from_raw(e.upstream_stage_id.0.to_native()),
            }),
            ArchivedDataSource::Stream(s) => DataSource::Stream(StreamSource {
                topic: s.topic.as_str().to_string(),
            }),
        }
    }
}

impl From<&ArchivedDataSink> for DataSink {
    fn from(archived: &ArchivedDataSink) -> Self {
        match archived {
            ArchivedDataSink::Client { schema } => DataSink::Client {
                schema: schema.iter().map(FieldSchema::from).collect(),
            },
            ArchivedDataSink::Discard => DataSink::Discard,
            ArchivedDataSink::Shuffle(s) => DataSink::Shuffle(ShuffleTarget {
                vfs_path_prefix: s.vfs_path_prefix.as_str().to_string(),
                num_partitions: s.num_partitions.to_native(),
            }),
            ArchivedDataSink::Storage(s) => DataSink::Storage(StorageTarget {
                destination_uri: s.destination_uri.as_str().to_string(),
            }),
        }
    }
}

impl From<&ArchivedExecuteTask> for ExecuteTask {
    fn from(archived: &ArchivedExecuteTask) -> Self {
        Self {
            mode: ExecutionMode::from(&archived.mode),
            task_id: TaskId::from_raw(archived.task_id.0.to_native()),
            query_id: QueryId::from_raw(archived.query_id.0.to_native()),
            stage_id: StageId::from_raw(archived.stage_id.0.to_native()),
            source: DataSource::from(&archived.source),
            program: MicroProgram::from(&archived.program),
            sink: DataSink::from(&archived.sink),
        }
    }
}

// ── Control-Plane Conversions ──────────────────────────────────────

impl From<&ArchivedNodeKind> for NodeKind {
    fn from(archived: &ArchivedNodeKind) -> Self {
        match archived {
            ArchivedNodeKind::Master => NodeKind::Master,
            ArchivedNodeKind::Executor => NodeKind::Executor,
            ArchivedNodeKind::Vcs => NodeKind::Vcs,
        }
    }
}

impl From<&ArchivedNodeLoad> for NodeLoad {
    fn from(archived: &ArchivedNodeLoad) -> Self {
        Self {
            active_tasks: archived.active_tasks.to_native(),
            cpu_percent: archived.cpu_percent.to_native(),
            memory_bytes: archived.memory_bytes.to_native(),
        }
    }
}

impl From<&ArchivedVeinControlEvent> for VeinControlEvent {
    fn from(archived: &ArchivedVeinControlEvent) -> Self {
        match archived {
            ArchivedVeinControlEvent::TableInvalidated {
                namespace,
                table_name,
                new_snapshot_id,
            } => VeinControlEvent::TableInvalidated {
                namespace: namespace.iter().map(|s| s.as_str().to_string()).collect(),
                table_name: table_name.as_str().to_string(),
                new_snapshot_id: new_snapshot_id.to_native(),
            },
            ArchivedVeinControlEvent::CatalogReady => VeinControlEvent::CatalogReady,
            ArchivedVeinControlEvent::TaskCompleted {
                task_id,
                query_id,
                row_count,
                bytes_read,
                duration_ns,
                result_bytes,
            } => VeinControlEvent::TaskCompleted {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                row_count: row_count.to_native(),
                bytes_read: bytes_read.to_native(),
                duration_ns: duration_ns.to_native(),
                result_bytes: result_bytes.as_ref().map(|b| b.as_slice().to_vec()),
            },
            ArchivedVeinControlEvent::DispatchTask(task) => {
                VeinControlEvent::DispatchTask(ExecuteTask::from(task))
            }
            ArchivedVeinControlEvent::TaskFailed {
                task_id,
                query_id,
                error,
            } => VeinControlEvent::TaskFailed {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                error: error.as_str().to_string(),
            },
            ArchivedVeinControlEvent::CancelTask { task_id, query_id } => {
                VeinControlEvent::CancelTask {
                    task_id: TaskId::from_raw(task_id.0.to_native()),
                    query_id: QueryId::from_raw(query_id.0.to_native()),
                }
            }
            ArchivedVeinControlEvent::ExplicitDrain {
                query_id,
                stage_id,
            } => VeinControlEvent::ExplicitDrain {
                query_id: QueryId::from_raw(query_id.0.to_native()),
                stage_id: StageId::from_raw(stage_id.0.to_native()),
            },
            ArchivedVeinControlEvent::Heartbeat {
                node_id,
                node_kind,
                timestamp_ms,
                load,
                advertise_addr,
            } => VeinControlEvent::Heartbeat {
                node_id: NodeId::from_raw(node_id.0.to_native()),
                node_kind: NodeKind::from(node_kind),
                timestamp_ms: timestamp_ms.to_native(),
                load: NodeLoad::from(load),
                advertise_addr: advertise_addr.as_str().to_string(),
            },
            ArchivedVeinControlEvent::HeartbeatAck => VeinControlEvent::HeartbeatAck,
            ArchivedVeinControlEvent::StageReady {
                query_id,
                stage_id,
                num_partitions,
            } => VeinControlEvent::StageReady {
                query_id: QueryId::from_raw(query_id.0.to_native()),
                stage_id: StageId::from_raw(stage_id.0.to_native()),
                num_partitions: num_partitions.to_native(),
            },
            ArchivedVeinControlEvent::DrainCompleted {
                query_id,
                stage_id,
            } => VeinControlEvent::DrainCompleted {
                query_id: QueryId::from_raw(query_id.0.to_native()),
                stage_id: StageId::from_raw(stage_id.0.to_native()),
            },
        }
    }
}
