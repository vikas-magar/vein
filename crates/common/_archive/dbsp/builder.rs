use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use error::Result;

use super::chunk::Chunk;
use super::circuit::{Circuit, CircuitEdge, CircuitInputHandle, CircuitOutputHandle, StreamType};
use super::incremental::IncrementalOperator;
use super::r#operator::{OperatorNode, SinkOperator, SourceOperator, UnaryOperator};
use super::stream::Stream;
use crate::identifiers::OperatorId;

/// A handle to a circuit input (for pushing data in).
#[derive(Clone)]
pub struct CircuitInput {
    pub(crate) operator_id: OperatorId,
}

/// A handle to a circuit output (for reading results out).
#[derive(Clone)]
pub struct CircuitOutput {
    operator_id: OperatorId,
}

/// Builder for constructing DBSP circuits.
///
/// Usage:
/// ```ignore
/// let mut builder = CircuitBuilder::new();
/// let input = builder.add_input("edges", schema.clone());
/// let filtered = builder.filter(&input, predicate);
/// let circuit = builder.build()?;
/// ```
pub struct CircuitBuilder {
    operators: Vec<OperatorNode>,
    edges: Vec<CircuitEdge>,
    inputs: Vec<CircuitInputHandle>,
    outputs: Vec<CircuitOutputHandle>,
    output_map: HashMap<OperatorId, CircuitOutput>,
    next_id: u64,
}

impl CircuitBuilder {
    /// Create a new circuit builder.
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            edges: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            output_map: HashMap::new(),
            next_id: 1,
        }
    }

    fn gen_id(&mut self) -> OperatorId {
        let id = OperatorId::from_raw(self.next_id);
        self.next_id += 1;
        id
    }

    /// Add an input source (data injected externally).
    pub fn add_input(&mut self, _name: &str, schema: SchemaRef) -> (CircuitInput, CircuitOutput) {
        let id = self.gen_id();
        let (producer, consumer) = Stream::new(64);

        let input_handle = CircuitInputHandle {
            operator_id: id,
            producer,
        };
        let output_handle = CircuitOutputHandle {
            operator_id: id,
            consumer,
        };
        let circuit_output = CircuitOutput { operator_id: id };

        self.inputs.push(input_handle);
        self.outputs.push(output_handle);
        self.output_map.insert(id, circuit_output.clone());

        let input = CircuitInput { operator_id: id };

        // Store a reference for wiring in build()
        (input, circuit_output)
    }

    /// Add a custom source operator.
    pub fn add_source(
        &mut self,
        _name: &str,
        source: impl SourceOperator,
    ) -> CircuitOutput {
        let id = source.operator_id();
        let (producer, consumer) = Stream::new(64);

        let node = OperatorNode::Source {
            op: Box::new(source),
            output: producer,
        };
        let output = CircuitOutput { operator_id: id };

        self.operators.push(node);
        self.outputs.push(CircuitOutputHandle {
            operator_id: id,
            consumer,
        });
        self.output_map.insert(id, output.clone());
        output
    }

    /// Add a unary operator with the given input and function.
    pub fn add_unary(
        &mut self,
        _name: &'static str,
        input: &CircuitOutput,
        op: impl UnaryOperator,
    ) -> CircuitOutput {
        let id = op.operator_id();
        let (out_producer, out_consumer) = Stream::new(64);

        let node = OperatorNode::Unary {
            op: Box::new(op),
            output: out_producer,
        };
        let output = CircuitOutput { operator_id: id };

        self.edges.push(CircuitEdge {
            from: input.operator_id,
            to: id,
            stream_type: StreamType::Data,
        });
        self.operators.push(node);
        self.outputs.push(CircuitOutputHandle {
            operator_id: id,
            consumer: out_consumer,
        });
        self.output_map.insert(id, output.clone());
        output
    }

    /// Wrap a function as an incremental operator.
    pub fn incrementalize<F, Fut>(
        &mut self,
        name: &'static str,
        input: &CircuitOutput,
        func: F,
        schema: SchemaRef,
    ) -> CircuitOutput
    where
        F: FnMut(Chunk) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Chunk>> + Send + 'static,
    {
        let id = self.gen_id();
        let op = IncrementalOperator::new(id, name, func, schema);
        self.add_unary(name, input, op)
    }

    /// Add a sink operator (consumes input without producing output).
    pub fn add_sink(&mut self, input: &CircuitOutput, sink: impl SinkOperator) {
        let id = sink.operator_id();
        let node = OperatorNode::Sink {
            op: Box::new(sink),
        };
        self.edges.push(CircuitEdge {
            from: input.operator_id,
            to: id,
            stream_type: StreamType::Data,
        });
        self.operators.push(node);
    }

    /// Build the circuit. Validates connectivity.
    pub fn build(self) -> Result<Circuit> {
        Circuit::new(self.operators, self.edges, self.inputs, self.outputs)
    }
}
