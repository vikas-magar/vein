import { ResultDelivery } from "./result-delivery.mjs";
import { validateTableEntries } from "./table-files.mjs";

export function createEngineRuntime({ loadModule, postMessage, now = () => performance.now() }) {
  let modulePromise;
  let engine;
  let busy = false;
  let activeRequest;
  let panicReported = false;
  let pendingQuery;
  const delivery = new ResultDelivery(postMessage, now);

  function send(data, message) {
    postMessage({ requestId: data.requestId, ...message });
  }

  async function load(data) {
    const entries = validateTableEntries(data.entries);
    send(data, { type: "progress", message: "Starting WebAssembly engine", progress: { stage: "starting" } });
    modulePromise ??= loadModule(data.wasmModuleUrl).then(async module => {
      await module.default();
      return module;
    }).catch(cause => {
      modulePromise = undefined;
      throw new Error(`Could not load the WebAssembly engine: ${cause.message ?? cause}`);
    });
    const module = await modulePromise;
    module.setPanicReporter?.(message => {
      panicReported = true;
      if (activeRequest) send(activeRequest, { type: "error", operation: activeRequest.type,
        fatal: true, message: `Engine execution failed: ${String(message).slice(0, 2048)}` });
    });
    const candidate = new module.BrowserEngine();
    let totalBytes = 0;
    try {
      if (typeof candidate.registerFile !== "function" || typeof candidate.takeIoStatistics !== "function"
        || candidate.resultProtocol?.() !== 1) {
        throw new Error("The browser engine package is outdated. Rebuild the WASM package and reload this page.");
      }
      for (let index = 0; index < entries.length; index += 1) {
        const { file, path } = entries[index];
        candidate.registerFile(path, file);
        totalBytes += file.size;
        if (index === 0 || index === entries.length - 1 || Math.floor((index + 1) * 100 / entries.length) !== Math.floor(index * 100 / entries.length)) {
          send(data, { type: "progress", message: `Registering ${index + 1} of ${entries.length} files`,
            progress: { stage: "registering", completed: index + 1, total: entries.length } });
        }
      }
    } catch (cause) {
      candidate.free?.();
      throw cause;
    }
    engine?.free?.();
    engine = candidate;
    send(data, { type: "ready", fileCount: engine.fileCount(), totalBytes,
      ioStatistics: engine.takeIoStatistics(),
      directoryName: data.directoryName,
      tableNames: [...new Set(entries.map(({ path }) => path.split("/")[0]))].sort() });
  }

  async function query(data) {
    if (!engine) throw new Error("Open a data directory before running a query.");
    send(data, { type: "progress", message: "Running query…", progress: { stage: "executing" } });
    engine.takeIoStatistics();
    const metrics = await delivery.run(engine, data);
    send(data, { type: "result", ...metrics, ioStatistics: engine.takeIoStatistics() });
  }

  async function receive(data) {
    if (delivery.acknowledge(data)) return;
    if (busy) {
      if (data.type === "inspect") { send(data, { type: "inspection", unavailable: true }); return; }
      if (data.type === "query" && activeRequest?.type === "inspect" && !pendingQuery) {
        pendingQuery = data; return;
      }
      send(data, { type: "error", operation: data.type, message: "The engine is busy. Wait for the current operation to finish." });
      return;
    }
    busy = true;
    activeRequest = data;
    panicReported = false;
    try {
      if (data.type === "load") await load(data);
      else if (data.type === "query") await query(data);
      else if (data.type === "inspect") {
        if (!engine || typeof engine.inspect !== "function") send(data, { type: "inspection", unavailable: true });
        else send(data, { type: "inspection", inspection: JSON.parse(await engine.inspect(data.query, data.prefix)) });
      }
      else send(data, { type: "error", operation: data.type, message: `Unknown worker action: ${data.type}` });
    } catch (cause) {
      if (!panicReported) send(data, { type: "error", operation: data.type, message: cause.message ?? String(cause) });
    } finally {
      busy = false;
      activeRequest = undefined;
      if (pendingQuery) { const next = pendingQuery; pendingQuery = undefined; void receive(next); }
    }
  }
  return receive;
}
