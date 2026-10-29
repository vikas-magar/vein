//! Cross-platform monotonic timing used by execution and diagnostics.
//!
//! Native targets use the standard library clock. Browser WebAssembly uses
//! `performance.now()` through wasm-bindgen because `std::time::Instant` is
//! unavailable on `wasm32-unknown-unknown`.

#[cfg(not(target_arch = "wasm32"))]
pub use std::time::Instant;

#[cfg(target_arch = "wasm32")]
mod browser {
    use std::time::Duration;

    use wasm_bindgen::prelude::wasm_bindgen;

    #[wasm_bindgen(inline_js = "export function vein_monotonic_now() { return globalThis.performance?.now() ?? Date.now(); }")]
    extern "C" {
        fn vein_monotonic_now() -> f64;
    }

    /// Browser monotonic timestamp compatible with the native `Instant` API.
    #[derive(Clone, Copy, Debug)]
    pub struct Instant(f64);

    impl Instant {
        /// Captures the current browser monotonic timestamp.
        pub fn now() -> Self {
            Self(vein_monotonic_now())
        }

        /// Returns the duration since this timestamp, clamped for clock anomalies.
        pub fn elapsed(self) -> Duration {
            Self::now().duration_since(self)
        }

        /// Returns the nonnegative duration between two browser timestamps.
        pub fn duration_since(self, earlier: Self) -> Duration {
            let elapsed_ms = (self.0 - earlier.0).max(0.0);
            if !elapsed_ms.is_finite() {
                return Duration::ZERO;
            }
            Duration::from_nanos((elapsed_ms * 1_000_000.0) as u64)
        }
    }
}

#[cfg(target_arch = "wasm32")]
pub use browser::Instant;
