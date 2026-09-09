//! Virtual register allocator for bytecode lowering.
//!
//! Provides [`RegisterAllocator`], a deterministic bump-pointer register allocator
//! that tracks peak register usage and supports watermark-based release for nested sub-expressions.

use common::protocol::RegId;
use common::Result;
use error::VeinError;

/// Bump-pointer virtual register allocator with watermark-based release.
///
/// Registers `0..input_columns-1` are pre-assigned to the chunk's physical input columns.
/// Intermediate expressions, loaded constants, and evaluation results are assigned
/// monotonically increasing register IDs up to `u8::MAX`.
#[derive(Debug, Clone)]
pub struct RegisterAllocator {
    next_reg: RegId,
    max_reg: RegId,
}

impl RegisterAllocator {
    /// Creates a new register allocator with `input_columns` registers pre-allocated.
    pub fn new(input_columns: u8) -> Self {
        Self {
            next_reg: input_columns,
            max_reg: input_columns,
        }
    }

    /// Allocates the next available virtual register.
    ///
    /// Returns an error if the expression requires more than 255 virtual registers.
    pub fn allocate(&mut self) -> Result<RegId> {
        if self.next_reg == u8::MAX {
            return Err(VeinError::planning(
                "Expression exceeds maximum virtual registers limit (255)",
                None,
            ));
        }
        let reg = self.next_reg;
        self.next_reg += 1;
        self.max_reg = self.max_reg.max(self.next_reg);
        Ok(reg)
    }

    /// Captures the current allocation watermark.
    #[inline]
    pub fn mark(&self) -> RegId {
        self.next_reg
    }

    /// Releases temporary registers allocated above `watermark` back to the free pool.
    #[inline]
    pub fn release_to(&mut self, watermark: RegId) {
        self.next_reg = watermark;
    }

    /// Total peak register count required by the program for pre-allocation.
    #[inline]
    pub fn total_registers(&self) -> u8 {
        self.max_reg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_allocator_bump_and_watermark() {
        let mut alloc = RegisterAllocator::new(2);
        assert_eq!(alloc.total_registers(), 2);

        let r2 = alloc.allocate().unwrap();
        assert_eq!(r2, 2);
        assert_eq!(alloc.total_registers(), 3);

        let mark = alloc.mark();
        assert_eq!(mark, 3);

        let r3 = alloc.allocate().unwrap();
        let r4 = alloc.allocate().unwrap();
        assert_eq!(r3, 3);
        assert_eq!(r4, 4);
        assert_eq!(alloc.total_registers(), 5);

        // Release back to watermark
        alloc.release_to(mark);
        assert_eq!(alloc.mark(), 3);
        assert_eq!(alloc.total_registers(), 5); // peak is preserved

        let r3_again = alloc.allocate().unwrap();
        assert_eq!(r3_again, 3);
    }
}
