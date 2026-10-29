use std::sync::Once;
use std::cell::RefCell;
use wasm_bindgen::prelude::*;

static INSTALL: Once = Once::new();

thread_local! {
    static REPORTER: RefCell<Option<js_sys::Function>> = const { RefCell::new(None) };
}

/// Reports a fatal Rust failure before its WASM trap obscures the cause.
#[wasm_bindgen(js_name = setPanicReporter)]
pub fn set_panic_reporter(reporter: js_sys::Function) {
    REPORTER.with(|slot| *slot.borrow_mut() = Some(reporter));
}

pub(super) fn install() {
    INSTALL.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            let message: String = info.to_string().chars().take(2048).collect();
            console_error(&message);
            REPORTER.with(|slot| {
                if let Some(reporter) = slot.borrow().as_ref() {
                    let _ = reporter.call1(&JsValue::NULL, &JsValue::from_str(&message));
                }
            });
        }));
    });
}

#[wasm_bindgen::prelude::wasm_bindgen]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_namespace = console, js_name = error)]
    fn console_error(message: &str);
}
