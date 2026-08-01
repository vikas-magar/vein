use std::collections::HashMap;

use error::{Result, VeinError};
use crate::cancellation::CancellationToken;
use crate::identifiers::{EpochId, OperatorId};

use super::chunk::Chunk;
use super::epoch::StreamPayload;
use super::r#operator::OperatorNode;
use super::stream::{StreamConsumer, StreamProducer};

/// Handle for pushing input data into a circuit (from external).
pub struct CircuitInputHandle {
    /// The operator that receives external input.
    pub operator_id: OperatorId,
    /// Producer end for external callers to push data into the circuit.
    pub producer: StreamProducer,
}

/// Handle for reading output data from a circuit (to external).
pub struct CircuitOutputHandle {
    /// The operator that produces output.
    pub operator_id: OperatorId,
    /// Consumer end for external callers to read data from the circuit.
    pub consumer: StreamConsumer,
}

/// Edge between two operators in the circuit.
#[derive(Debug, Clone)]
pub struct CircuitEdge {
    /// Source operator ID.
    pub from: OperatorId,
    /// Target operator ID.
    pub to: OperatorId,
    /// Whether this is a forward data edge or a feedback edge.
    pub stream_type: StreamType,
}

/// Whether a circuit edge is forward data or feedback (for z⁻¹ loops).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamType {
    /// Forward data edge: output of upstream → input of downstream.
    Data,
    /// Feedback edge: output of downstream → input of upstream (for z⁻¹ loops).
    Feedback,
}

/// A DBSP circuit: DAG of operators connected by streams.
///
/// # Execution model
/// Epoch-driven: on each step(), input deltas are pushed through the circuit
/// in topological order. Feedback edges (strict operators) run BEFORE inputs.
pub struct Circuit {
    operators: Vec<OperatorNode>,
    edges: Vec<CircuitEdge>,
    inputs: Vec<CircuitInputHandle>,
    outputs: Vec<CircuitOutputHandle>,
    current_raw: u64,
    cancel: CancellationToken,
}

impl Circuit {
    /// Create a new circuit with the given topology.
    pub fn new(
        operators: Vec<OperatorNode>,
        edges: Vec<CircuitEdge>,
        inputs: Vec<CircuitInputHandle>,
        outputs: Vec<CircuitOutputHandle>,
    ) -> Result<Self> {
        if edges.is_empty() && operators.len() > 1 {
            return Err(VeinError::internal(
                "circuit with multiple operators must have edges",
            ));
        }
        Ok(Self {
            operators,
            edges,
            inputs,
            outputs,
            current_raw: 1,
            cancel: CancellationToken::new(),
        })
    }

    /// Get an input handle by operator ID.
    pub fn input(&self, operator_id: OperatorId) -> Option<&CircuitInputHandle> {
        self.inputs.iter().find(|h| h.operator_id == operator_id)
    }

    /// Get an output handle by operator ID.
    pub fn output(&self, operator_id: OperatorId) -> Option<&CircuitOutputHandle> {
        self.outputs.iter().find(|h| h.operator_id == operator_id)
    }

    /// Execute ONE epoch. Returns output diffs.
    pub async fn step(
        &mut self,
        inputs: Vec<(OperatorId, Chunk)>,
    ) -> Result<Vec<(OperatorId, Chunk)>> {
        let epoch = self.current_raw;

        // Pre-compute upstream mapping
        let upstream: HashMap<OperatorId, Vec<OperatorId>> = self
            .edges
            .iter()
            .fold(HashMap::new(), |mut map, e| {
                map.entry(e.to).or_default().push(e.from);
                map
            });

        // Output accumulator: maps operator_id -> its output chunk for this epoch
        let mut epoch_outputs: HashMap<OperatorId, Chunk> = HashMap::new();

        // Route external inputs: push to input stream AND record in epoch_outputs
        for (op_id, chunk) in inputs {
            if let Some(handle) = self.inputs.iter_mut().find(|h| h.operator_id == op_id) {
                handle
                    .producer
                    .push(StreamPayload::Data(chunk.clone()))
                    .await?;
            }
            epoch_outputs.insert(op_id, chunk);
        }

        // ┌─ Phase 0: Strict (feedback) ──
        for node in &mut self.operators {
            if let OperatorNode::Strict { op, output } = node {
                let id = op.operator_id();
                op.clock_start(EpochId::from_raw(epoch)).await?;
                let result = op.eval_strict().await?;
                output.push(StreamPayload::Data(result.clone())).await?;
                epoch_outputs.insert(id, result);
                op.clock_end(EpochId::from_raw(epoch)).await?;
            }
        }

        // ┌─ Phase 1: Source ──
        for node in &mut self.operators {
            if let OperatorNode::Source { op, output } = node {
                let id = op.operator_id();
                op.clock_start(EpochId::from_raw(epoch)).await?;
                let result = op.eval().await?;
                output.push(StreamPayload::Data(result.clone())).await?;
                epoch_outputs.insert(id, result);
                op.clock_end(EpochId::from_raw(epoch)).await?;
            }
        }

        // ┌─ Phase 2: Unary / Binary (internal operators) ──
        for node in &mut self.operators {
            match node {
                OperatorNode::Unary { op, output } => {
                    let id = op.operator_id();
                    let up_ids = upstream.get(&id).cloned().unwrap_or_default();
                    op.clock_start(EpochId::from_raw(epoch)).await?;

                    let input_chunk = up_ids.iter().find_map(|up_id| epoch_outputs.get(up_id).cloned());
                    if let Some(chunk) = input_chunk {
                        let result = op.eval(&chunk).await?;
                        output.push(StreamPayload::Data(result.clone())).await?;
                        epoch_outputs.insert(id, result);
                    }
                    op.clock_end(EpochId::from_raw(epoch)).await?;
                }
                OperatorNode::Binary { op, output } => {
                    let id = op.operator_id();
                    let up_ids = upstream.get(&id).cloned().unwrap_or_default();
                    op.clock_start(EpochId::from_raw(epoch)).await?;

                    let (lhs_chunk, rhs_chunk) = if up_ids.len() >= 2 {
                        (epoch_outputs.get(&up_ids[0]).cloned(), epoch_outputs.get(&up_ids[1]).cloned())
                    } else {
                        (None, None)
                    };

                    if let (Some(lc), Some(rc)) = (lhs_chunk, rhs_chunk) {
                        let result = op.eval(&lc, &rc).await?;
                        output.push(StreamPayload::Data(result.clone())).await?;
                        epoch_outputs.insert(id, result);
                    }
                    op.clock_end(EpochId::from_raw(epoch)).await?;
                }
                _ => {}
            }
        }

        // ┌─ Phase 3: Sink ──
        for node in &mut self.operators {
            if let OperatorNode::Sink { op } = node {
                let id = op.operator_id();
                let up_ids = upstream.get(&id).cloned().unwrap_or_default();
                op.clock_start(EpochId::from_raw(epoch)).await?;
                for up_id in up_ids {
                    if let Some(chunk) = epoch_outputs.get(&up_id) {
                        op.eval(chunk).await?;
                    }
                }
                op.clock_end(EpochId::from_raw(epoch)).await?;
            }
        }

        // Collect outputs from all output handles
        let mut outputs = Vec::new();
        for handle in &mut self.outputs {
            let payload = handle.consumer.next().await?;
            if let Some(StreamPayload::Data(chunk)) = payload {
                outputs.push((handle.operator_id, chunk));
            }
        }

        self.current_raw += 1;
        Ok(outputs)
    }

    /// Run continuously (streaming mode). Blocks until cancelled.
    /// Reads input from all input handles (external sources) each epoch.
    pub async fn run(&mut self) -> Result<()> {
        // Map from operator_id to its input consumer (reversed from input handle)
        let _input_consumers: HashMap<OperatorId, StreamConsumer> = HashMap::new();
        // This requires storing the consumer ends during circuit construction.
        // For now, step() is the primary API.
        

        loop {
            if self.cancel.is_cancelled() {
                break Ok(());
            }
            // In a full implementation, we'd read from input handles' streams here.
            // For now, step() with explicit input vec is the API.
            self.step(Vec::new()).await?;
        }
    }

    /// Current epoch.
    pub fn current_epoch(&self) -> EpochId {
        EpochId::from_raw(self.current_raw - 1)
    }

    /// Cancel the circuit.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}

// ── DOT rendering ──

impl std::fmt::Display for Circuit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "digraph Circuit {{")?;
        for node in &self.operators {
            let id = node.operator_id();
            let name = node.name();
            writeln!(f, "  op_{} [label=\"{}\"];", id.into_raw(), name)?;
        }
        for edge in &self.edges {
            let style = match edge.stream_type {
                StreamType::Data => "",
                StreamType::Feedback => " style=dashed",
            };
            writeln!(
                f,
                "  op_{} -> op_{}[{}];",
                edge.from.into_raw(),
                edge.to.into_raw(),
                style
            )?;
        }
        writeln!(f, "}}")
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use async_trait::async_trait;
    use crate::dbsp::chunk::Chunk;
    use crate::dbsp::r#operator::{DbspOperator, UnaryOperator};
    use crate::identifiers::OperatorId;

    struct AddOneOp {
        id: OperatorId,
    }

    #[async_trait]
    impl DbspOperator for AddOneOp {
        fn name(&self) -> &'static str { "add_one" }
        fn operator_id(&self) -> OperatorId { self.id }
        async fn init(&mut self) -> Result<()> { Ok(()) }
    }

    #[async_trait]
    impl UnaryOperator for AddOneOp {
        async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
            Ok(input.clone())
        }
    }

    #[tokio::test]
    async fn test_circuit_dot_render() {
        let id = OperatorId::from_raw(1);
        let (producer, _consumer) = super::super::stream::Stream::new(16);

        let op = AddOneOp { id };
        let node = OperatorNode::Unary {
            op: Box::new(op),
            output: producer,
        };

        let result = Circuit::new(vec![node], vec![], vec![], vec![]);
        assert!(result.is_ok());
        let _dot = result.unwrap().to_string();
    }

    #[tokio::test]
    async fn test_circuit_step() {
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use crate::dbsp::builder::CircuitBuilder;

        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]));

        let mut builder = CircuitBuilder::new();
        let (input, output) = builder.add_input("numbers", schema.clone());

        let _filtered = builder.add_unary("pass_through", &output, AddOneOp { id: OperatorId::from_raw(100) });

        let mut circuit = builder.build().unwrap();

        let col = arrow::array::Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap();
        let chunk = Chunk::from_records(batch, 1);

        let results = circuit.step(vec![(input.operator_id, chunk)]).await.unwrap();
        assert!(!results.is_empty(), "expected at least one output");
        // Should have output for both the input handle and the unary operator
        assert_eq!(results.len(), 2);
    }
}
