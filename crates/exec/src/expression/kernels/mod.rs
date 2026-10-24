//! Arrow vector kernels shared by typed expression evaluation and scalar UDFs.

pub mod arithmetic;
pub mod cast;
pub mod comparison;
pub mod conditional;
pub mod constant;
pub mod logic;
mod mapped_kernel;
mod mapped_predicate;
pub(crate) mod mapped_comparison;
pub(crate) mod mapped_cast;
pub(crate) mod wide_integer;

pub(crate) use conditional::{eval_case_value, eval_in_list_value};
pub(crate) use mapped_kernel::{eval_mapped_binary_value, eval_mapped_unary_value};
pub(crate) use mapped_predicate::{eval_mapped_boolean_binary, eval_mapped_unary_predicate};
pub(crate) use logic::{eval_and_datum, eval_or_datum};
pub use arithmetic::{eval_add, eval_div, eval_mod, eval_mul, eval_neg, eval_sub};
pub use cast::eval_cast;
pub use comparison::{
    eval_cmp_eq, eval_cmp_gt, eval_cmp_gte, eval_cmp_lt, eval_cmp_lte, eval_cmp_neq, eval_like,
};
pub use constant::literal_to_scalar;
pub use logic::{eval_is_not_null, eval_is_null, eval_not};
