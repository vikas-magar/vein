use super::*;

pub(in crate::runtime::tests) enum InputPlanFixture {
    Pipeline {
        source: DataSource,
        program: Arc<ExpressionProgram>,
    },
    Transform {
        input: Box<InputPlanFixture>,
        program: Arc<ExpressionProgram>,
    },
    Aggregate {
        input: Box<InputPlanFixture>,
        plan: AggregatePlan,
    },
    Distinct {
        input: Box<InputPlanFixture>,
        plan: AggregatePlan,
    },
    Sort {
        input: Box<InputPlanFixture>,
        plan: SortPlan,
        key_program: Option<Arc<ExpressionProgram>>,
        output_columns: u32,
    },
    Limit {
        input: Box<InputPlanFixture>,
        spec: LimitSpec,
    },
    Join {
        probe: Box<InputPlanFixture>,
        build: Box<InputPlanFixture>,
        plan: JoinPlan,
    },
}

/// A builder whose only difference from the default is that it can
/// materialize a `DataSource::Memory` task, which the default builder
/// rejects because the wire variant carries no payload.
pub(in crate::runtime::tests) struct MemoryFeedBuilder {
    pub(in crate::runtime::tests) inner: crate::factory::DefaultExecutorBuilder,
    pub(in crate::runtime::tests) values: Vec<Option<i64>>,
}

impl MemoryFeedBuilder {
    pub(in crate::runtime::tests) fn new() -> Self {
        Self::with_values(vec![1, 2, 3, 4, 5])
    }

    pub(in crate::runtime::tests) fn with_values(values: Vec<i64>) -> Self {
        Self::with_nullable_values(values.into_iter().map(Some).collect())
    }

    pub(in crate::runtime::tests) fn with_nullable_values(values: Vec<Option<i64>>) -> Self {
        Self {
            inner: crate::factory::DefaultExecutorBuilder::new(Arc::new(
                functions::FunctionRegistry::new(),
            )),
            values,
        }
    }
}

impl ExecutorBuilder for MemoryFeedBuilder {
    fn build_source(
        &self,
        source: &DataSource,
        task_id: TaskId,
        attempt: u32,
    ) -> Result<Box<dyn Source>> {
        if !matches!(source, DataSource::Memory) {
            return self.inner.build_source(source, task_id, attempt);
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("dummy", arrow::datatypes::DataType::Int64, true),
        ]));
        let values = Arc::new(arrow::array::Int64Array::from(self.values.clone()))
            as arrow::array::ArrayRef;
        let row_count = self.values.len();
        let view = DataChunkView::from_arrays(
            vec![values],
            row_count,
            common::arrow::SelectionVector::new_dense(row_count),
        )?
        .with_schema(schema)?;
        Ok(Box::new(MemorySource::new(view)))
    }

    fn build_expression_executor(
        &self,
        program: &Arc<ExpressionProgram>,
        next: Box<dyn PipelineNode>,
    ) -> Result<Box<dyn Transform>> {
        self.inner.build_expression_executor(program, next)
    }

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink>> {
        self.inner.build_sink(sink)
    }

    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<DataChunkView>,
    ) -> Result<Box<dyn Sink>> {
        self.inner.build_streaming_sink(sink, output)
    }
}
