use crate::daemon::pipeline::{DataBlock, FactorizedChunk};
use common::Result;

/// An in-memory table that stores factorized data blocks without flattening.
/// This acts as the payload storage for Hash Joins.
pub struct FactorizedTable {
    /// The raw data blocks stored in this table.
    /// In a real implementation, these would be laid out contiguously in memory
    /// pages, but we use a Vec of DataBlocks for this prototype.
    pub blocks: Vec<DataBlock>,
    /// Total number of rows stored across all blocks.
    pub num_rows: usize,
}

impl FactorizedTable {
    /// Creates a new, empty `FactorizedTable`.
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
            num_rows: 0,
        }
    }

    /// Appends a new DataBlock to the factorized table.
    pub fn append(&mut self, block: DataBlock) -> Result<()> {
        self.num_rows += block.selection.size();
        self.blocks.push(block);
        Ok(())
    }

    /// Simulates looking up a match and appending it to an incoming chunk
    /// to produce a zero-copy factorized join result.
    pub fn lookup_and_append(&self, chunk: &mut FactorizedChunk, match_idx: usize) -> Result<()> {
        if match_idx < self.blocks.len() {
            // Zero-copy: just clone the pointer/Arc to the block
            // (Since DataBlock wraps Arc'd arrow Arrays in ValueVector, cloning it is a shallow copy)
            chunk.blocks.push(self.blocks[match_idx].clone());
        }
        Ok(())
    }
}

impl Default for FactorizedTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::arrow::SelectionVector;

    #[test]
    fn test_append() {
        let mut table = FactorizedTable::new();
        assert_eq!(table.num_rows, 0);

        let block = DataBlock {
            vectors: vec![],
            selection: SelectionVector::new_dense(10),
        };
        table.append(block).unwrap();

        assert_eq!(table.num_rows, 10);
        assert_eq!(table.blocks.len(), 1);
    }

    #[test]
    fn test_lookup_and_append() {
        let mut table = FactorizedTable::new();
        table
            .append(DataBlock {
                vectors: vec![],
                selection: SelectionVector::new_dense(10),
            })
            .unwrap();
        table
            .append(DataBlock {
                vectors: vec![],
                selection: SelectionVector::new_dense(5),
            })
            .unwrap();

        let mut chunk = FactorizedChunk { blocks: Vec::new() };

        // valid match
        table.lookup_and_append(&mut chunk, 1).unwrap();
        assert_eq!(chunk.blocks.len(), 1);
        assert_eq!(chunk.blocks[0].selection.size(), 5);

        // invalid match, just does nothing
        table.lookup_and_append(&mut chunk, 99).unwrap();
        assert_eq!(chunk.blocks.len(), 1);
    }
}
