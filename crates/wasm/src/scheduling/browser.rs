//! Browser task-queue yielding for the shared cooperative driver.
use common::{Result, VeinError};
use exec::pipeline::resumable::host::CooperativeHost;
use super::pending::PendingYield;
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{MessageChannel, MessageEvent};

type Pending = Rc<RefCell<PendingYield>>;

/// A reusable browser task-queue adapter; no query data crosses this channel.
///
/// A MessageChannel task allows Worker I/O, ACK and cancellation events to run.
/// Promise-only yielding would drain microtasks without granting that opportunity.
pub struct BrowserHost {
    channel: MessageChannel,
    pending: Pending,
    _callback: Closure<dyn FnMut(MessageEvent)>,
}

impl BrowserHost {
    /// Allocates one task channel for a cooperative execution host.
    pub fn new() -> Result<Self> {
        let channel = MessageChannel::new().map_err(browser_error)?;
        let pending = Rc::new(RefCell::new(PendingYield::default()));
        let slot = Rc::clone(&pending);
        let callback = Closure::wrap(Box::new(move |event: MessageEvent| {
            let sender = event.data().as_f64().and_then(|ticket| slot.borrow_mut().complete(ticket));
            if let Some(sender) = sender { let _ = sender.send(()); }
        }) as Box<dyn FnMut(MessageEvent)>);
        channel.port1().set_onmessage(Some(callback.as_ref().unchecked_ref()));
        channel.port1().start();
        Ok(Self { channel, pending, _callback: callback })
    }
}

impl CooperativeHost for BrowserHost {
    async fn yield_control(&mut self) -> Result<()> {
        let (ticket, receiver) = self.pending.borrow_mut().start()?;
        let _reset = PendingReset(Rc::clone(&self.pending), ticket);
        self.channel.port2().post_message(&JsValue::from(ticket)).map_err(browser_error)?;
        receiver.await.map_err(|_| VeinError::internal("browser scheduling channel closed"))
    }
}

struct PendingReset(Pending, u32);
impl Drop for PendingReset {
    fn drop(&mut self) {
        let retired = self.0.borrow_mut().cancel(self.1);
        drop(retired);
    }
}

impl Drop for BrowserHost {
    fn drop(&mut self) {
        self.channel.port1().set_onmessage(None);
        self.channel.port1().close();
        self.channel.port2().close();
        let retired = self.pending.borrow_mut().clear();
        drop(retired);
    }
}

fn browser_error(error: JsValue) -> VeinError {
    VeinError::internal(format!("browser scheduling channel failed: {error:?}"))
}
