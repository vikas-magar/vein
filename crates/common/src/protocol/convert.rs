//! Zero-copy From<&Archived*> conversions for all protocol wire types.
//!
//! Provides fast O(1) in-place extraction from validated rkyv buffers without
//! lifetime infection or rkyv::Deserialize Pool strategy mismatches.

use super::control::{
    TableWritePartDescriptor,
    ArchivedNodeKind, ArchivedNodeLoad, ArchivedRaftRpcEnvelope, ArchivedRaftRpcKind,
    ArchivedVeinControlEvent, NodeKind, NodeLoad, RaftRpcEnvelope, RaftRpcKind, VeinControlEvent,
};
use super::exchange::{ArchivedExchangeKey, ExchangeKey};
use super::ir::constant::{ArchivedScalarConstant, ScalarConstant};
use super::ir::op::{ArchivedMicroOp, MicroOp};
use super::ir::program::{ArchivedMicroProgram, MicroProgram};
use super::ir::state::{
    AggOpCode, ArchivedAggOpCode, ArchivedJoinKind, ArchivedStateMode, JoinKind, StateMode,
};
use super::join::{
    ArchivedExchangeInput, ArchivedJoinKeyMode, ArchivedJoinKeySpec, ArchivedJoinSource,
    ArchivedPhysicalJoinType, ExchangeInput, JoinKeyMode, JoinKeySpec, JoinSource,
    PhysicalJoinType,
};
use super::task::{
    ArchivedCatalogSnapshotToken, ArchivedDataSink, ArchivedDataSource, ArchivedExecuteTask,
    ArchivedExecutionMode, ArchivedFieldSchema, ArchivedFileFormat, ArchivedFileScanTask,
    ArchivedScanPredicate,
    ArchivedScanPredicateValue, CatalogSnapshotToken, DataSink, DataSource, ExchangeSource,
    ExecuteTask, ExecutionMode,
    FieldSchema, FileFormat, FileScanTask, KeyValue, ScanPredicate, ScanPredicateValue,
    ShuffleTarget, StorageTarget, StreamSource,
};
use crate::identifiers::{NodeId, QueryId, RequestId, StageId, TaskId};

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
            ArchivedFileFormat::Exo => FileFormat::Exo,
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

impl From<&ArchivedScanPredicateValue> for ScanPredicateValue {
    fn from(archived: &ArchivedScanPredicateValue) -> Self {
        match archived {
            ArchivedScanPredicateValue::Boolean(value) => Self::Boolean(*value),
            ArchivedScanPredicateValue::Int64(value) => Self::Int64(value.to_native()),
            ArchivedScanPredicateValue::Float64(value) => Self::Float64(value.to_native()),
            ArchivedScanPredicateValue::String(value) => Self::String(value.as_str().to_string()),
            ArchivedScanPredicateValue::Date(value) => Self::Date(value.to_native()),
            ArchivedScanPredicateValue::Timestamp(value) => Self::Timestamp(value.to_native()),
        }
    }
}

impl From<&ArchivedScanPredicate> for ScanPredicate {
    fn from(archived: &ArchivedScanPredicate) -> Self {
        match archived {
            ArchivedScanPredicate::Address {
                column,
                operator,
                value,
            } => Self::Address {
                column: column.as_str().to_string(),
                operator: operator.as_str().to_string(),
                value: ScanPredicateValue::from(value),
            },
            ArchivedScanPredicate::AddressNull { column, is_null } => Self::AddressNull {
                column: column.as_str().to_string(),
                is_null: *is_null,
            },
            ArchivedScanPredicate::AddressIn { column, values } => Self::AddressIn {
                column: column.as_str().to_string(),
                values: values.iter().map(ScanPredicateValue::from).collect(),
            },
        }
    }
}

impl From<&ArchivedCatalogSnapshotToken> for CatalogSnapshotToken {
    fn from(archived: &ArchivedCatalogSnapshotToken) -> Self {
        Self::new(
            archived.table_id.as_str(),
            archived.catalog_revision.as_str(),
            archived.snapshot_id.as_str(),
            archived.schema_version.to_native(),
        )
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
            scan_predicates: archived
                .scan_predicates
                .iter()
                .map(ScanPredicate::from)
                .collect(),
            task_id: TaskId::from_raw(archived.task_id.0.to_native()),
            catalog_snapshot: archived
                .catalog_snapshot
                .as_ref()
                .map(CatalogSnapshotToken::from),
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

impl From<&ArchivedPhysicalJoinType> for PhysicalJoinType {
    fn from(archived: &ArchivedPhysicalJoinType) -> Self {
        match archived {
            ArchivedPhysicalJoinType::Inner => Self::Inner,
            ArchivedPhysicalJoinType::Left => Self::Left,
            ArchivedPhysicalJoinType::Right => Self::Right,
            ArchivedPhysicalJoinType::Full => Self::Full,
            ArchivedPhysicalJoinType::Cross => Self::Cross,
        }
    }
}

impl From<&ArchivedJoinKeyMode> for JoinKeyMode {
    fn from(archived: &ArchivedJoinKeyMode) -> Self {
        match archived {
            ArchivedJoinKeyMode::Auto => Self::Auto,
            ArchivedJoinKeyMode::AddressNative => Self::AddressNative,
            ArchivedJoinKeyMode::ValueNative => Self::ValueNative,
        }
    }
}

impl From<&ArchivedJoinKeySpec> for JoinKeySpec {
    fn from(archived: &ArchivedJoinKeySpec) -> Self {
        Self {
            field_id: archived.field_id.to_native(),
            column_name: archived.column_name.as_str().to_string(),
            mode: JoinKeyMode::from(&archived.mode),
        }
    }
}

impl From<&ArchivedExchangeKey> for ExchangeKey {
    fn from(archived: &ArchivedExchangeKey) -> Self {
        Self {
            query_id: QueryId::from_raw(archived.query_id.0.to_native()),
            stage_id: StageId::from_raw(archived.stage_id.0.to_native()),
        }
    }
}

impl From<&ArchivedExchangeInput> for ExchangeInput {
    fn from(archived: &ArchivedExchangeInput) -> Self {
        Self {
            exchange_key: ExchangeKey::from(&archived.exchange_key),
            upstream_stage_id: StageId::from_raw(archived.upstream_stage_id.0.to_native()),
            path_prefix: archived.path_prefix.as_str().to_string(),
            partition_id: archived.partition_id.to_native(),
            partition_count: archived.partition_count.to_native(),
        }
    }
}

impl From<&ArchivedJoinSource> for JoinSource {
    fn from(archived: &ArchivedJoinSource) -> Self {
        Self {
            left: ExchangeInput::from(&archived.left),
            right: ExchangeInput::from(&archived.right),
            join_type: PhysicalJoinType::from(&archived.join_type),
            left_keys: archived.left_keys.iter().map(JoinKeySpec::from).collect(),
            right_keys: archived.right_keys.iter().map(JoinKeySpec::from).collect(),
            requires_exact_verification: archived.requires_exact_verification,
            left_schema: archived.left_schema.iter().map(FieldSchema::from).collect(),
            right_schema: archived
                .right_schema
                .iter()
                .map(FieldSchema::from)
                .collect(),
        }
    }
}

impl From<&ArchivedDataSource> for DataSource {
    fn from(archived: &ArchivedDataSource) -> Self {
        match archived {
            ArchivedDataSource::Memory => DataSource::Memory,
            ArchivedDataSource::File(scan) => DataSource::File(FileScanTask::from(scan)),
            ArchivedDataSource::Exchange(e) => DataSource::Exchange(ExchangeSource {
                exchange_key: ExchangeKey::from(&e.exchange_key),
                upstream_stage_id: StageId::from_raw(e.upstream_stage_id.0.to_native()),
                path_prefix: e.path_prefix.as_str().to_string(),
                partition_id: e.partition_id.to_native(),
                partition_count: e.partition_count.to_native(),
                schema: e.schema.iter().map(FieldSchema::from).collect(),
            }),
            ArchivedDataSource::Join(join) => DataSource::Join(JoinSource::from(join)),
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
                exchange_key: ExchangeKey::from(&s.exchange_key),
                vfs_path_prefix: s.vfs_path_prefix.as_str().to_string(),
                producer_task_id: TaskId::from_raw(s.producer_task_id.0.to_native()),
                attempt: s.attempt.to_native(),
                expected_tasks: s
                    .expected_tasks
                    .iter()
                    .map(|task| TaskId::from_raw(task.0.to_native()))
                    .collect(),
                schema_fingerprint: s.schema_fingerprint.as_str().to_string(),
                num_partitions: s.num_partitions.to_native(),
                partition_keys: s.partition_keys.iter().map(JoinKeySpec::from).collect(),
                schema: s.schema.iter().map(FieldSchema::from).collect(),
            }),
            ArchivedDataSink::Storage(s) => DataSink::Storage(StorageTarget {
                destination_uri: s.destination_uri.as_str().to_string(),
                options: s
                    .options
                    .iter()
                    .map(|kv| KeyValue {
                        key: kv.key.as_str().to_string(),
                        value: kv.value.as_str().to_string(),
                    })
                    .collect(),
                schema: s.schema.iter().map(FieldSchema::from).collect(),
            }),
        }
    }
}

impl From<&ArchivedExecuteTask> for ExecuteTask {
    fn from(archived: &ArchivedExecuteTask) -> Self {
        Self {
            request_id: RequestId::from_raw(archived.request_id.0.to_native()),
            mode: ExecutionMode::from(&archived.mode),
            task_id: TaskId::from_raw(archived.task_id.0.to_native()),
            attempt: archived.attempt.to_native(),
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

impl From<&ArchivedRaftRpcKind> for RaftRpcKind {
    fn from(archived: &ArchivedRaftRpcKind) -> Self {
        match archived {
            ArchivedRaftRpcKind::Vote => RaftRpcKind::Vote,
            ArchivedRaftRpcKind::AppendEntries => RaftRpcKind::AppendEntries,
            ArchivedRaftRpcKind::InstallSnapshot => RaftRpcKind::InstallSnapshot,
        }
    }
}

impl From<&ArchivedRaftRpcEnvelope> for RaftRpcEnvelope {
    fn from(archived: &ArchivedRaftRpcEnvelope) -> Self {
        Self {
            request_id: archived.request_id.to_native(),
            kind: RaftRpcKind::from(&archived.kind),
            payload: archived.payload.as_slice().to_vec(),
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
                attempt,
                row_count,
                bytes_read,
                duration_ns,
                result_bytes,
                write_parts,
            } => VeinControlEvent::TaskCompleted {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                attempt: attempt.to_native(),
                row_count: row_count.to_native(),
                bytes_read: bytes_read.to_native(),
                duration_ns: duration_ns.to_native(),
                result_bytes: result_bytes.as_ref().map(|b| b.as_slice().to_vec()),
                write_parts: write_parts.iter().map(|part| TableWritePartDescriptor {
                    partition_id: part.partition_id.to_native(),
                    artifact_id: part.artifact_id.as_str().to_owned(),
                    artifact_uri: part.artifact_uri.as_str().to_owned(),
                    checksum: part.checksum.as_str().to_owned(),
                    size_bytes: part.size_bytes.to_native(),
                    row_count: part.row_count.to_native(),
                }).collect(),
            },
            ArchivedVeinControlEvent::TaskResultStart {
                task_id,
                query_id,
                attempt,
            } => VeinControlEvent::TaskResultStart {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                attempt: attempt.to_native(),
            },
            ArchivedVeinControlEvent::TaskResultBatch {
                task_id,
                query_id,
                attempt,
                sequence,
                result_bytes,
            } => VeinControlEvent::TaskResultBatch {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                attempt: attempt.to_native(),
                sequence: sequence.to_native(),
                result_bytes: result_bytes.as_slice().to_vec(),
            },
            ArchivedVeinControlEvent::TaskResultEnd {
                task_id,
                query_id,
                attempt,
                row_count,
                bytes_read,
                duration_ns,
            } => VeinControlEvent::TaskResultEnd {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                attempt: attempt.to_native(),
                row_count: row_count.to_native(),
                bytes_read: bytes_read.to_native(),
                duration_ns: duration_ns.to_native(),
            },
            ArchivedVeinControlEvent::RaftRpcRequest(envelope) => {
                VeinControlEvent::RaftRpcRequest(RaftRpcEnvelope::from(envelope))
            }
            ArchivedVeinControlEvent::RaftRpcResponse { envelope, error } => {
                VeinControlEvent::RaftRpcResponse {
                    envelope: RaftRpcEnvelope::from(envelope),
                    error: error.as_ref().map(|value| value.as_str().to_string()),
                }
            }
            ArchivedVeinControlEvent::DispatchTask(task) => {
                VeinControlEvent::DispatchTask(ExecuteTask::from(task))
            }
            ArchivedVeinControlEvent::TaskFailed {
                task_id,
                query_id,
                attempt,
                error,
            } => VeinControlEvent::TaskFailed {
                task_id: TaskId::from_raw(task_id.0.to_native()),
                query_id: QueryId::from_raw(query_id.0.to_native()),
                attempt: attempt.to_native(),
                error: error.as_str().to_string(),
            },
            ArchivedVeinControlEvent::CancelTask { task_id, query_id } => {
                VeinControlEvent::CancelTask {
                    task_id: TaskId::from_raw(task_id.0.to_native()),
                    query_id: QueryId::from_raw(query_id.0.to_native()),
                }
            }
            ArchivedVeinControlEvent::ExplicitDrain { query_id, stage_id } => {
                VeinControlEvent::ExplicitDrain {
                    query_id: QueryId::from_raw(query_id.0.to_native()),
                    stage_id: StageId::from_raw(stage_id.0.to_native()),
                }
            }
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
            ArchivedVeinControlEvent::DrainCompleted { query_id, stage_id } => {
                VeinControlEvent::DrainCompleted {
                    query_id: QueryId::from_raw(query_id.0.to_native()),
                    stage_id: StageId::from_raw(stage_id.0.to_native()),
                }
            }
        }
    }
}
