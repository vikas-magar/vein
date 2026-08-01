use std::marker::PhantomData;

use async_trait::async_trait;
use arrow::datatypes::SchemaRef;
use error::Result;

use super::arrow_hash_index::ArrowHashIndex;
use super::chunk::Chunk;
use super::r#operator::{DbspOperator, UnaryOperator};
use crate::identifiers::OperatorId;
use crate::pipeline::PipelineStage;

/// The I (integrate) operator: accumulates input diffs into full state.
///
/// I(d) = z⁻¹(state) + d
///
/// On epoch 1, z⁻¹ is empty, so I(d) = d (full initial data).
pub struct Integrator {
    state: ArrowHashIndex,
}

impl Integrator {
    /// Create a new integrator with the given schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            state: ArrowHashIndex::new(schema),
        }
    }

    /// Apply a diff and return the full accumulated state.
    /// I(d) = z⁻¹(state) + d
    pub async fn integrate(&mut self, diff: &Chunk) -> Result<Chunk> {
        self.state.apply_diff(diff)?;
        self.state.snapshot()
    }
}

/// The D (differentiate) operator: computes output diff from full output.
///
/// D(r) = r - z⁻¹(prev_output)
///
/// On epoch 1, z⁻¹ is empty, so D(r) = r (full result).
pub struct Differentiator {
    prev_output: ArrowHashIndex,
}

impl Differentiator {
    /// Create a new differentiator with the given schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            prev_output: ArrowHashIndex::new(schema),
        }
    }

    /// Compute the output diff.
    /// D(r) = r - z⁻¹(prev_output)
    pub async fn differentiate(&mut self, full_output: Chunk) -> Result<Chunk> {
        let prev = self.prev_output.snapshot()?;
        let diff = if prev.len() == 0 {
            full_output.clone()
        } else {
            full_output.minus(&prev)?
        };
        self.prev_output.apply_diff(&full_output)?;
        Ok(diff)
    }
}

/// Wraps any PipelineStage into a streaming UnaryOperator.
///
/// f_Δ(d) = D ∘ f ∘ I(d)
///
/// This is Algorithm 4.1 from the DBSP paper, applied mechanically.
/// On epoch 1, z⁻¹(state) and z⁻¹(prev_out) are both empty, so:
///   I(d) = ∅ + d = d
///   D(r) = r - ∅ = r
/// The first epoch is equivalent to running the batch query directly.
pub struct IncrementalOperator<F, Fut> {
    id: OperatorId,
    name: &'static str,
    /// The inner batch function: takes a Chunk, returns a Chunk.
    func: F,
    integrator: Integrator,
    differentiator: Differentiator,
    _phantom: PhantomData<Fut>,
}

impl<F, Fut> IncrementalOperator<F, Fut>
where
    F: FnMut(Chunk) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Chunk>> + Send,
{
    /// Create a new incremental operator wrapping a batch function.
    pub fn new(
        id: OperatorId,
        name: &'static str,
        func: F,
        schema: SchemaRef,
    ) -> Self {
        Self {
            id,
            name,
            func,
            integrator: Integrator::new(schema.clone()),
            differentiator: Differentiator::new(schema),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut> DbspOperator for IncrementalOperator<F, Fut>
where
    F: FnMut(Chunk) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Chunk>> + Send + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn operator_id(&self) -> OperatorId {
        self.id
    }

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl<F, Fut> UnaryOperator for IncrementalOperator<F, Fut>
where
    F: FnMut(Chunk) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Chunk>> + Send + 'static,
{
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk> {
        let full_state = self.integrator.integrate(input).await?;
        let output = (self.func)(full_state).await?;
        let diff = self.differentiator.differentiate(output).await?;
        Ok(diff)
    }
}

// ── IncrementalPipelineStage ──

/// Wraps a batch function as a PipelineStage that processes Chunks incrementally.
///
/// f_Δ(d) = D ∘ f ∘ I(d)
///
/// Each call to `process(delta)`:
///   1. Integrates the delta into full state: I(d) = z⁻¹(state) + d
///   2. Applies the batch function to the full state
///   3. Differentiates: D(r) = r - z⁻¹(prev_out)
///   4. Returns the output delta
///
/// On the first call, z⁻¹(state) and z⁻¹(prev_out) are both empty,
/// so the output equals f(input) directly.
pub struct IncrementalPipelineStage<F, Fut> {
    name: String,
    func: F,
    integrator: Integrator,
    differentiator: Differentiator,
    _phantom: PhantomData<Fut>,
}

impl<F, Fut> IncrementalPipelineStage<F, Fut>
where
    F: FnMut(Chunk) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Chunk>> + Send + 'static,
{
    /// Create a new incremental pipeline stage wrapping a batch function.
    pub fn new(name: impl Into<String>, func: F, schema: SchemaRef) -> Self {
        Self {
            name: name.into(),
            func,
            integrator: Integrator::new(schema.clone()),
            differentiator: Differentiator::new(schema),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut> PipelineStage for IncrementalPipelineStage<F, Fut>
where
    F: FnMut(Chunk) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<Chunk>> + Send + 'static,
{
    type Input = Chunk;
    type Output = Chunk;

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process(&mut self, input: Chunk) -> Result<Option<Chunk>> {
        let full_state = self.integrator.integrate(&input).await?;
        let output = (self.func)(full_state).await?;
        let diff = self.differentiator.differentiate(output).await?;
        Ok(Some(diff))
    }

    async fn finish(&mut self) -> Result<Option<Chunk>> {
        Ok(None)
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use crate::dbsp::chunk::Chunk;
    use crate::dbsp::r#operator::UnaryOperator;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
        ]))
    }

    #[tokio::test]
    async fn test_incremental_first_epoch() {
        let schema = schema();
        let id = OperatorId::from_raw(1);

        let mut op = IncrementalOperator::new(id, "pass_through", |chunk: Chunk| async move {
            Ok(chunk)
        }, schema.clone());

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let input = Chunk::from_records(batch, 1);

        let output = op.eval(&input).await.unwrap();
        assert_eq!(output.len(), 3);
        assert_eq!(output.weighted_count(), 3);
    }

    #[tokio::test]
    async fn test_incremental_pipeline_stage_direct() {
        let schema = schema();

        let mut stage = IncrementalPipelineStage::new(
            "counter",
            |chunk: Chunk| async move { Ok(chunk) },
            schema.clone(),
        );

        stage.init().await.unwrap();

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let input = Chunk::from_records(batch, 1);

        // First epoch: same as input (z⁻¹ empty)
        let result = stage.process(input.clone()).await.unwrap();
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r.weighted_count(), 3);
    }
}
