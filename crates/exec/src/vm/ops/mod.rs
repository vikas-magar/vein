//! Instruction kernels and operations for the Vector VM.
//!
//! Submodules implement isolated execution logic for arithmetic, comparison,
//! boolean logic, constant materialization, filter masking, UDFs, and AI vectors.

pub mod arithmetic;
pub mod cast;
pub mod comparison;
pub mod conditional;
pub(crate) use conditional::eval_in_list_value;
pub mod constant;
pub mod logic;
mod mapped_predicate;
mod mapped_kernel;
pub mod mask;
pub mod udf;
pub mod vector_dist;

pub use arithmetic::{eval_add, eval_div, eval_mod, eval_mul, eval_neg, eval_sub};
pub(crate) use arithmetic::{eval_mapped_arithmetic, eval_mapped_neg};
pub use cast::eval_cast;
pub(crate) use cast::eval_mapped_cast;
pub use comparison::{
    eval_cmp_eq, eval_cmp_gt, eval_cmp_gte, eval_cmp_lt, eval_cmp_lte, eval_cmp_neq, eval_like,
};
pub use conditional::{eval_case, eval_in_list};
pub(crate) use conditional::eval_case_value;
pub use constant::{literal_to_array, literal_to_scalar};
pub use logic::{eval_and, eval_is_not_null, eval_is_null, eval_not, eval_or};
pub(crate) use logic::{eval_and_datum, eval_or_datum};
pub(crate) use mapped_predicate::{eval_mapped_boolean_binary, eval_mapped_unary_predicate};
pub(crate) use mapped_kernel::{eval_mapped_binary_value, eval_mapped_unary_value};
pub use mask::apply_filter_mask;
pub use udf::{eval_call_udf, eval_call_udf_mapped};
pub use vector_dist::{eval_cosine_distance, eval_dot_product, eval_l2_distance};
pub(crate) use vector_dist::{eval_mapped_vector_distance, VectorDistance};
