import assert from "node:assert/strict";
import test from "node:test";
import { createEngineRuntime } from "../../public/worker/engine-runtime.mjs";
import { tableFiles } from "../../public/worker/table-files.mjs";

function fixture({ failRegister = false, failInit = false, panic = false, protocol = 1 } = {}) {
  const messages = [];
  const engines = [];
  let reportPanic;
  class Engine {
    constructor() { this.paths = []; this.freed = false; engines.push(this); }
    registerFile(path) { if (failRegister) throw new Error("File registration rejected"); this.paths.push(path); }
    fileCount() { return this.paths.length; }
    resultProtocol() { return protocol; }
    takeIoStatistics() { return { readCalls: 0, requestedBytes: 0, bytesRead: 0, maxReadBytes: 0 }; }
    free() { this.freed = true; }
    async query(query, consume) {
      if (panic) {
        reportPanic("checkpoint statistics: missing host runtime");
        throw new WebAssembly.RuntimeError("unreachable");
      }
      await consume(new Uint8Array([9, 1, 2, 8]).subarray(1, 3));
    }
  }
  const receive = createEngineRuntime({
    loadModule: async () => ({ default: async () => { if (failInit) throw new Error("WASM startup failed"); },
      setPanicReporter: reporter => { reportPanic = reporter; }, BrowserEngine: Engine }),
    postMessage: (data, transfer) => {
      messages.push({ data, transfer });
      if (data.type === "result-batch") queueMicrotask(() => receive({ type: "result-ack",
        requestId: data.requestId, sequence: data.sequence, accepted: true }));
    },
  });
  const files = [{ name: "part.parquet", webkitRelativePath: "tpch/part/part.parquet", size: 3,
    arrayBuffer() { throw new Error("File payload must not be read during registration"); } }];
  return { receive, entries: tableFiles(files), messages, engines };
}

test("Worker load and query messages preserve request IDs and exact Arrow byte ranges", async () => {
  const { receive, entries, messages, engines } = fixture();
  await receive({ type: "load", entries, directoryName: "tpch", wasmModuleUrl: "/wasm/engine.js", requestId: 1 });
  const ready = messages.at(-1).data;
  assert.deepEqual(engines[0].paths, ["part/part.parquet"]);
  assert.deepEqual(ready, { requestId: 1, type: "ready", fileCount: 1, totalBytes: 3, directoryName: "tpch", tableNames: ["part"],
    ioStatistics: { readCalls: 0, requestedBytes: 0, bytesRead: 0, maxReadBytes: 0 } });
  const registration = messages.find(({ data }) => data.progress?.stage === "registering");
  assert.deepEqual(registration.data.progress, { stage: "registering", completed: 1, total: 1 });
  await receive({ type: "query", query: "FROM part", requestId: 2 });
  const result = messages.find(({ data }) => data.type === "result-batch");
  assert.equal(result.data.requestId, 2);
  assert.deepEqual([...new Uint8Array(result.data.buffer)], [1, 2]);
  assert.equal(result.transfer[0], result.data.buffer);
  assert.ok(messages.at(-1).data.engineMs >= 0);
  assert.equal(messages.at(-1).data.transport.maxPacketBytes, 2);
  const execution = messages.find(({ data }) => data.requestId === 2 && data.type === "progress");
  assert.deepEqual(execution.data.progress, { stage: "executing" });
});

test("Rust panic detail is preserved instead of overwritten by the WASM trap", async () => {
  const { receive, entries, messages } = fixture({ panic: true });
  await receive({ type: "load", entries, directoryName: "tpch", requestId: 1 });
  await receive({ type: "query", query: "FROM part", requestId: 2 });
  const errors = messages.filter(({ data }) => data.type === "error");
  assert.equal(errors.length, 1);
  assert.equal(errors[0].data.requestId, 2);
  assert.equal(errors[0].data.fatal, true);
  assert.match(errors[0].data.message, /checkpoint statistics: missing host runtime/);
});

test("a failed registration frees the partial engine and never announces readiness", async () => {
  const { receive, entries, messages, engines } = fixture({ failRegister: true });
  await receive({ type: "load", entries, directoryName: "tpch", requestId: 3 });
  assert.equal(engines[0].freed, true);
  assert.ok(messages.every(({ data }) => data.type !== "ready"));
  assert.match(messages.at(-1).data.message, /File registration rejected/);
  await receive({ type: "query", query: "FROM part", requestId: 4 });
  assert.match(messages.at(-1).data.message, /Open a data directory/);
});

test("WASM initialization failures identify the failed operation", async () => {
  const { receive, entries, messages } = fixture({ failInit: true });
  await receive({ type: "load", entries, directoryName: "tpch", requestId: 5 });
  assert.equal(messages.at(-1).data.operation, "load");
  assert.match(messages.at(-1).data.message, /Could not load the WebAssembly engine: WASM startup failed/);
});

test("an outdated WASM package is rejected and freed before announcing readiness", async () => {
  const { receive, entries, messages, engines } = fixture({ protocol: 0 });
  await receive({ type: "load", entries, requestId: 1 });
  assert.equal(engines[0].freed, true);
  assert.equal(messages.some(({ data }) => data.type === "ready"), false);
  assert.match(messages.at(-1).data.message, /outdated/);
});

test("an executing inspection serializes WASM access and gives the next query priority", async () => {
  const { receive, entries, messages, engines } = fixture();
  await receive({ type: "load", entries, directoryName: "tpch", requestId: 1 });
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  engines[0].inspect = async () => {
    await gate;
    return JSON.stringify({ columns: [], functions: [], diagnostics: [] });
  };
  const inspecting = receive({ type: "inspect", query: "FROM part", prefix: "FROM part", requestId: "inspect:1" });
  await receive({ type: "query", query: "FROM part", requestId: 2 });
  await receive({ type: "inspect", query: "FROM part", requestId: "inspect:2" });
  assert.equal(messages.at(-1).data.unavailable, true);
  release(); await inspecting;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(messages.filter(({ data }) => data.type === "result" && data.requestId === 2).length, 1);
  assert.equal(messages.filter(({ data }) => data.type === "error").length, 0);
});
