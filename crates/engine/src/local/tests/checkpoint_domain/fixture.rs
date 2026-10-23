//! Reference-backed source fixtures for full adaptive-execution regression tests.
use super::super::*;
use std::collections::HashMap;

pub(super) type Columns = Vec<(&'static str, Vec<Option<i64>>)>;

pub(super) struct Tables {
    pub metadata: HashMap<String, Arc<TableMetadata>>,
    pub views: HashMap<String, DataChunkView>,
}

#[async_trait]
impl CatalogProvider for Tables {
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>> {
        self.metadata.get(name).cloned().ok_or_else(|| VeinError::invalid_input(name))
    }

    fn metadata_store(&self) -> MetadataStore { MetadataStore::new() }
}

pub(super) struct MemoryProvider {
    pub tables: Arc<Tables>,
    pub format: DataFileFormatId,
}

#[async_trait]
impl ScanProvider for MemoryProvider {
    fn format_id(&self) -> &DataFileFormatId { &self.format }

    async fn execute(&self, request: ScanRequest<'_>, next: &mut dyn PipelineNode) -> Result<()> {
        for file in request.files {
            let name = file.absolute.file_stem().unwrap().to_str().unwrap();
            let view = &self.tables.views[name];
            let schema = &self.tables.metadata[name].schema;
            let columns = request.projected.fields.iter().map(|column| {
                schema.fields.iter().position(|field| field.name == column.name).unwrap()
            }).collect::<Vec<_>>();
            // Static fixture predicates are tautologies over these values.
            // Runtime filters may be ignored: the join still enforces membership.
            next.push(view.project(&columns)?)?;
        }
        Ok(())
    }
}

pub(super) fn tables(root: &std::path::Path, definitions: Vec<(&str, Columns)>) -> Arc<Tables> {
    let format = DataFileFormatId::new("domain_fixture");
    let mut metadata = HashMap::new();
    let mut views = HashMap::new();
    for (name, columns) in definitions {
        let rows = columns[0].1.len();
        let arrow_schema = Arc::new(Schema::new(columns.iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, true)).collect::<Vec<_>>()));
        let arrays = columns.iter().map(|(_, values)|
            Arc::new(Int64Array::from(values.clone())) as arrow::array::ArrayRef).collect();
        let view = DataChunkView::from_arrays(arrays, rows, SelectionVector::new_dense(rows))
            .unwrap().with_schema(arrow_schema).unwrap();
        let schema = Arc::new(VeinSchema::new(columns.iter().enumerate().map(|(id, (name, _))|
            Arc::new(ResolvedColumn::new(ColumnId(id), *name, None, VeinDataType::Int64)
                .with_nullable(true))).collect()));
        let file = root.join(format!("{name}.fixture"));
        std::fs::File::create(&file).unwrap();
        let table = TableMetadata::new(name, schema)
            .with_location(root.to_string_lossy())
            .with_file_format(format.clone())
            .with_files(vec![file.to_string_lossy().into_owned()])
            .with_statistics(catalog::TableStatistics { num_rows: Some(rows as u64), ..Default::default() });
        metadata.insert(name.into(), Arc::new(table));
        views.insert(name.into(), view);
    }
    Arc::new(Tables { metadata, views })
}
