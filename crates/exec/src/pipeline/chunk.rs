//! In-flight columnar data chunk representation.

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use common::arrow::SelectionVector;
use exo::AddressBatch;
use common::Result;
use error::VeinError;

/// A horizontal slice of data processed in a pipeline.
///
/// The single in-flight unit flowing through a pipeline: a set of Arrow
/// columns plus a [`SelectionVector`] selecting the active rows. Columns are
/// shared by `Arc` (zero-copy) — no data is copied as the chunk moves through
/// operators.
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// The columns in this chunk, each an Arc'd Arrow array.
    pub vectors: Vec<ArrayRef>,
    /// The selection vector indicating active rows.
    pub selection: SelectionVector,
    /// Optional canonical-address sidecars aligned with the physical vectors.
    /// `None` means the source is value-native or the column has no address plane.
    pub address_columns: Vec<Option<AddressBatch>>,
}

impl DataChunk {
    /// Creates a new [`DataChunk`] from column arrays and an explicit selection vector.
    pub fn new(vectors: Vec<ArrayRef>, selection: SelectionVector) -> Self {
        Self {
            address_columns: vec![None; vectors.len()],
            vectors,
            selection,
        }
    }

    /// Creates a chunk with canonical address sidecars aligned to its columns.
    pub fn from_vectors_with_addresses(
        vectors: Vec<ArrayRef>,
        address_columns: Vec<Option<AddressBatch>>,
    ) -> Result<Self> {
        if vectors.len() != address_columns.len() {
            return Err(VeinError::internal(format!(
                "data chunk address sidecars {} do not match vectors {}",
                address_columns.len(),
                vectors.len()
            )));
        }
        let physical_len = vectors.first().map(|vector| vector.len()).unwrap_or(0);
        if address_columns.iter().flatten().any(|batch| batch.len() != physical_len) {
            return Err(VeinError::internal(
                "data chunk address sidecar length does not match vectors",
            ));
        }
        Ok(Self {
            vectors,
            selection: SelectionVector::new_dense(physical_len),
            address_columns,
        })
    }

    /// Creates a [`DataChunk`] from column arrays with an all-valid (dense) selection vector.
    pub fn from_vectors(vectors: Vec<ArrayRef>) -> Self {
        let num_rows = vectors.first().map(|v| v.len()).unwrap_or(0);
        Self {
            address_columns: vec![None; vectors.len()],
            vectors,
            selection: SelectionVector::new_dense(num_rows),
        }
    }

    /// Creates a [`DataChunk`] from an Arrow [`RecordBatch`] with an initially dense selection.
    pub fn from_record_batch(batch: RecordBatch) -> Self {
        let num_rows = batch.num_rows();
        let vectors = batch.columns().to_vec();
        Self {
            address_columns: vec![None; vectors.len()],
            vectors,
            selection: SelectionVector::new_dense(num_rows),
        }
    }

    /// Constructs an ephemeral Arrow [`RecordBatch`] from chunk vectors.
    /// Used for interoperability with Arrow compute kernels that require schema metadata.
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let fields: Vec<arrow::datatypes::Field> = self
            .vectors
            .iter()
            .enumerate()
            .map(|(i, vec)| {
                arrow::datatypes::Field::new(format!("col_{i}"), vec.data_type().clone(), true)
            })
            .collect();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(schema, self.vectors.clone())
            .map_err(|e| VeinError::internal(format!("Failed to create RecordBatch from DataChunk: {e}")))
    }

    /// Total surviving rows in this chunk.
    pub fn num_rows(&self) -> usize {
        self.selection.size()
    }

    /// The physical length of the underlying column vectors in this chunk.
    pub fn physical_len(&self) -> usize {
        self.vectors.first().map(|v| v.len()).unwrap_or(0)
    }

    /// Number of column vectors in this chunk.
    pub fn num_columns(&self) -> usize {
        self.vectors.len()
    }

    /// Whether this chunk has zero surviving rows.
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    /// Borrow the column array at `index`.
    pub fn column(&self, index: usize) -> &ArrayRef {
        &self.vectors[index]
    }
}
