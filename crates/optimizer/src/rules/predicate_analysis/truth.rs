//! Sets of possible SQL truth values, with exact three-valued composition.

#[derive(Clone, Copy)]
pub(super) struct TruthSet(u8);

impl TruthSet {
    pub(super) const TRUE: Self = Self(1);
    pub(super) const FALSE: Self = Self(2);
    pub(super) const NULL: Self = Self(4);
    pub(super) const BOOLEAN: Self = Self(3);
    pub(super) const NON_TRUE: Self = Self(6);
    pub(super) const ANY: Self = Self(7);

    pub(super) fn can_be_true(self) -> bool {
        self.0 & Self::TRUE.0 != 0
    }

    pub(super) fn not(self) -> Self {
        Self(((self.0 & 1) << 1) | ((self.0 & 2) >> 1) | (self.0 & 4))
    }

    pub(super) fn combine(self, other: Self, and: bool) -> Self {
        let mut result = 0;
        for left in [1, 2, 4] {
            for right in [1, 2, 4] {
                if self.0 & left != 0 && other.0 & right != 0 {
                    result |= if and {
                        if left == 2 || right == 2 { 2 } else if left == 4 || right == 4 { 4 } else { 1 }
                    } else if left == 1 || right == 1 { 1 } else if left == 4 || right == 4 { 4 } else { 2 };
                }
            }
        }
        Self(result)
    }
}
