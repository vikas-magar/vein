//! JavaScript-facing runtime backed by selected browser file references.

mod engine;
pub(crate) mod files;
mod panic_hook;
mod query;
mod inspection;

pub use engine::BrowserEngine;
pub use crate::scheduling::BrowserHost;
