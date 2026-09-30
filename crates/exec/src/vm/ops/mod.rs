//! Instruction kernels and operations for the Vector VM.
//!
//! Submodules implement isolated execution logic for arithmetic, comparison,
//! boolean logic, constant materialization, filter masking, UDFs, and AI vectors.

pub mod arithmetic;
pub mod cast;
pub mod comparison;
pub mod constant;
pub mod logic;
pub mod mask;
pub mod udf;
pub mod vector_dist;

pub use arithmetic::{eval_add, eval_div, eval_mod, eval_mul, eval_neg, eval_sub};
pub use cast::eval_cast;
pub use comparison::{
    eval_cmp_eq, eval_cmp_gt, eval_cmp_gte, eval_cmp_lt, eval_cmp_lte, eval_cmp_neq,
};
pub use constant::{scalar_constant_to_array, scalar_constant_to_scalar};
pub use logic::{eval_and, eval_is_not_null, eval_is_null, eval_not, eval_or};
pub use mask::apply_filter_mask;
pub use udf::eval_call_udf;
pub use vector_dist::{eval_cosine_distance, eval_dot_product, eval_l2_distance};
