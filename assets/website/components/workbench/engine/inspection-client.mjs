/** Bounded metadata RPC channel; never changes query execution state or retains Arrow results. */
export class InspectionClient {
  constructor(worker, ready) {
    this.worker = worker; this.ready = ready; this.sequence = 0;
    this.active = null; this.queued = null; this.cache = new Map();
  }
  inspect(query, prefix = query) {
    if (!this.ready() || !this.worker()) return Promise.resolve(null);
    const key = `${query}\0${prefix}`;
    if (this.cache.has(key)) return Promise.resolve(this.cache.get(key));
    if (this.active?.key === key) return this.active.promise;
    if (this.queued?.key === key) return this.queued.promise;
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    const request = { key, query, prefix, resolve, promise, requestId: `inspect:${++this.sequence}` };
    if (this.active) {
      this.queued?.resolve(null); this.queued = request;
    } else this.send(request);
    return promise;
  }
  send(request) {
    if (!this.ready() || !this.worker()) { request.resolve(null); return; }
    this.active = request;
    request.timer = setTimeout(() => this.finish(null), 10_000);
    try { this.worker().postMessage({ type: "inspect", query: request.query,
      prefix: request.prefix, requestId: request.requestId }); }
    catch { this.finish(null); }
  }
  receive(data) {
    if (!String(data.requestId).startsWith("inspect:")) return false;
    if (data.requestId !== this.active?.requestId) return true;
    const value = data.type === "inspection" && !data.unavailable ? data.inspection : null;
    if (value && [value.columns, value.functions, value.diagnostics].every(Array.isArray)) {
      this.cache.set(this.active.key, value);
      if (this.cache.size > 8) this.cache.delete(this.cache.keys().next().value);
      this.finish(value);
    } else this.finish(null);
    return true;
  }
  finish(value) {
    if (!this.active) return;
    clearTimeout(this.active.timer); this.active.resolve(value); this.active = null;
    const queued = this.queued; this.queued = null;
    if (queued) this.send(queued);
  }
  cancel() {
    if (this.active) { clearTimeout(this.active.timer); this.active.resolve(null); }
    this.queued?.resolve(null); this.active = null; this.queued = null; this.cache.clear();
  }
}
