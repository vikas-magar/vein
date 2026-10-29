import { tableFiles } from "../../../public/worker/table-files.mjs";
import { InspectionClient } from "./inspection-client.mjs";
import { ResultCollector } from "./result-collector.mjs";
import { assembleArrowTables } from "../results/assemble.mjs";

export function initialEngineState() {
  return { phase: "idle", message: "Open a data directory to begin", error: "", operation: null, dataset: null, result: null, progress: null };
}

// One owner for Worker lifetime, request identity, and query state.
export class EngineClient {
  constructor({ workerFactory, wasmModuleUrl, decodeResult, combineResults = assembleArrowTables, onChange, now = () => performance.now() }) {
    this.workerFactory = workerFactory;
    this.wasmModuleUrl = wasmModuleUrl;
    this.decodeResult = decodeResult;
    this.combineResults = combineResults;
    this.onChange = onChange;
    this.now = now;
    this.state = initialEngineState();
    this.worker = null;
    this.requestId = 0;
    this.inspection = new InspectionClient(() => this.worker, () => this.state.phase === "ready");
  }

  update(changes) {
    this.state = { ...this.state, ...changes };
    this.onChange(this.state);
  }

  load(files, sourceId) {
    this.stopWorker();
    this.update({ ...initialEngineState(), phase: "loading", operation: "load", message: "Starting browser engine", progress: { stage: "starting" } });
    const requestId = ++this.requestId;
    this.loadingSourceId = sourceId ?? `selection:${requestId}`;
    try {
      const entries = tableFiles(files);
      const directoryName = entries[0].file.webkitRelativePath.split("/")[0];
      const worker = this.workerFactory();
      this.worker = worker;
      worker.onmessage = ({ data }) => this.receive(worker, data);
      worker.onerror = event => {
        if (this.worker !== worker) return;
        event.preventDefault?.();
        this.fail(`The browser engine stopped${event.message ? `: ${event.message}` : "."} Open the data directory again to reconnect.`, true);
      };
      worker.onmessageerror = () => {
        if (this.worker === worker) this.fail("The browser could not read an engine message. Open the data directory again.", true);
      };
      worker.postMessage({ type: "load", entries, directoryName, wasmModuleUrl: this.wasmModuleUrl, requestId });
      return true;
    } catch (cause) {
      this.fail(`Could not start the browser engine: ${cause.message ?? cause}`, true);
      return false;
    }
  }

  run(query) {
    if (this.state.phase !== "ready" || !this.worker || !query.trim()) return false;
    this.startedAt = this.now();
    this.runningQuery = query;
    this.runningSource = this.state.dataset;
    const requestId = ++this.requestId;
    this.update({ phase: "running", operation: "query", message: "Executing query", error: "", result: null, progress: { stage: "executing" } });
    this.collector = new ResultCollector(this.decodeResult, this.combineResults);
    this.inspection.cancel();
    try {
      this.worker.postMessage({ type: "query", query, requestId });
      return true;
    } catch (cause) {
      this.fail(`Could not send the query: ${cause.message ?? cause}`);
      return false;
    }
  }

  async receive(worker, data) {
    if (this.worker !== worker) return;
    if (this.inspection.receive(data)) {
      if (data.fatal) this.fail(data.message, true);
      return;
    }
    if (data.requestId !== this.requestId) return;
    if (data.type === "progress") return this.update({ message: data.message, progress: data.progress ?? null });
    if (data.type === "ready" && this.state.phase === "loading") {
      return this.update({ phase: "ready", error: "", operation: null, message: "Ready", progress: null,
        dataset: Object.freeze({ id: this.loadingSourceId, name: data.directoryName, files: data.fileCount,
          bytes: data.totalBytes, tables: Object.freeze(data.tableNames) }) });
    }
    if (data.type === "error") return this.fail(data.message, data.fatal === true || this.state.phase === "loading");
    if (data.type === "result-batch" && this.state.phase === "running") {
      const collector = this.collector;
      try {
        if (this.state.progress?.stage !== "decoding") {
          this.update({ message: "Decoding query result", progress: { stage: "decoding" } });
        }
        await collector.append(data);
        if (this.worker !== worker || data.requestId !== this.requestId || collector !== this.collector) {
          collector.release(); return;
        }
        worker.postMessage({ type: "result-ack", requestId: data.requestId, sequence: data.sequence, accepted: true });
      } catch (cause) {
        if (this.worker === worker && data.requestId === this.requestId) {
          worker.postMessage({ type: "result-ack", requestId: data.requestId, sequence: data.sequence,
            accepted: false, message: cause.message ?? String(cause) });
          this.fail(`Could not decode the query result: ${cause.message ?? cause}`);
        }
      }
      return;
    }
    if (data.type !== "result" || this.state.phase !== "running") return;
    try {
      const { table, byteLength } = this.collector.finish(data.transport);
      this.collector.release(); this.collector = null;
      this.update({ phase: "ready", message: "Query completed", error: "", progress: null, result: {
        byteLength, table, transport: data.transport, query: this.runningQuery, source: this.runningSource,
        elapsedMs: this.now() - this.startedAt, engineMs: data.engineMs, ioStatistics: data.ioStatistics,
      } });
    } catch (cause) {
      if (this.worker === worker && data.requestId === this.requestId) {
        this.fail(`Could not decode the query result: ${cause.message ?? cause}`);
      }
    }
  }

  fail(message, fatal = false) {
    this.collector?.release(); this.collector = null;
    if (fatal) this.stopWorker();
    this.update({ phase: fatal ? "error" : "ready", error: String(message), message: "Operation failed", progress: null,
      ...(fatal ? { dataset: null } : {}) });
  }

  clearResult() {
    if (this.state.phase === "running" || this.state.phase === "loading") return;
    this.update({ result: null, error: "", message: this.state.dataset ? "Ready" : "Open a data directory to begin" });
  }

  cancel() {
    this.stopWorker();
    this.update({ ...initialEngineState(), message: "Stopped. Reopen the data directory to continue." });
  }

  stopWorker() {
    this.collector?.release(); this.collector = null;
    this.inspection.cancel();
    if (!this.worker) return;
    this.worker.onmessage = null;
    this.worker.onerror = null;
    this.worker.onmessageerror = null;
    this.worker.terminate();
    this.worker = null;
  }

  dispose() {
    this.stopWorker();
    this.onChange = () => {};
  }
}
