import assert from "node:assert/strict";
import test from "node:test";
import { EngineClient } from "../../components/workbench/engine/client.mjs";

function fixture(decodeResult = async buffer => buffer) {
  const workers = [];
  const client = new EngineClient({ wasmModuleUrl: "/wasm/engine.js", decodeResult, onChange: () => {},
    workerFactory: () => {
      const worker = { messages: [], terminated: false, postMessage(message) { this.messages.push(message); },
        terminate() { this.terminated = true; }, emit(data) { return this.onmessage?.({ data }); } };
      workers.push(worker);
      return worker;
    } });
  const files = [{ name: "part.parquet", webkitRelativePath: "tpch/part/part.parquet" }];
  const ready = async worker => worker.emit({ type: "ready", requestId: client.requestId,
    directoryName: "tpch", fileCount: 1, totalBytes: 50, tableNames: ["part"] });
  return { client, workers, files, ready };
}
async function complete(worker, client, buffer) {
  const requestId = client.requestId;
  await worker.emit({ type: "result-batch", requestId, sequence: 0, buffer });
  await worker.emit({ type: "result", requestId, transport: { packets: 1, byteLength: buffer.byteLength } });
}

test("a query error preserves loaded data and permits a corrected retry", async () => {
  const { client, workers, files, ready } = fixture();
  client.load(files); await ready(workers[0]);
  assert.equal(client.run("FROM part"), true);
  await workers[0].emit({ type: "error", requestId: client.requestId, message: "Unknown column" });
  assert.equal(client.state.phase, "ready");
  assert.deepEqual(client.state.dataset.tables, ["part"]);
  assert.equal(client.state.error, "Unknown column");
  assert.equal(client.run("FROM part |> SELECT p_partkey"), true);
  assert.equal(client.state.error, "");
});

test("replacing a Worker rejects late ready, progress and result messages", async () => {
  const { client, workers, files } = fixture();
  client.load(files);
  const receiveOld = workers[0].onmessage;
  const oldId = client.requestId;
  client.load(files);
  for (const type of ["ready", "progress", "result"]) await receiveOld({ data: { type, requestId: oldId, message: "stale" } });
  assert.equal(workers[0].terminated, true);
  assert.equal(client.state.phase, "loading");
  assert.equal(client.state.dataset, null);
  assert.notEqual(client.state.message, "stale");
});

test("a fatal Worker error stops once without an automatic restart loop", () => {
  const { client, workers, files } = fixture();
  client.load(files);
  workers[0].onerror({ message: "WebAssembly unavailable", preventDefault() {} });
  assert.equal(workers.length, 1);
  assert.equal(workers[0].terminated, true);
  assert.equal(client.state.phase, "error");
  assert.match(client.state.error, /WebAssembly unavailable/);
});

test("a reported Rust panic stops the damaged engine and retains its actual cause", async () => {
  const { client, workers, files, ready } = fixture();
  client.load(files); await ready(workers[0]);
  client.run("FROM part");
  const onError = workers[0].onerror;
  await workers[0].emit({ type: "error", requestId: client.requestId, fatal: true,
    message: "Engine execution failed: checkpoint statistics: missing host runtime" });
  onError({ message: "unreachable", preventDefault() {} });
  assert.equal(client.state.phase, "error");
  assert.equal(workers[0].terminated, true);
  assert.match(client.state.error, /checkpoint statistics: missing host runtime/);
  assert.equal(client.run("FROM part"), false);
});

test("cancelling during asynchronous result decoding cannot publish a stale result", async () => {
  let finish;
  const { client, workers, files, ready } = fixture(() => new Promise(resolve => { finish = resolve; }));
  client.load(files); await ready(workers[0]); client.run("FROM part");
  const pending = workers[0].emit({ type: "result-batch", requestId: client.requestId, sequence: 0, buffer: new ArrayBuffer(0) });
  assert.equal(client.state.progress.stage, "decoding");
  client.cancel(); finish("stale table"); await pending;
  assert.equal(client.state.phase, "idle");
  assert.equal(client.state.result, null);
  assert.equal(client.state.progress, null);
});

test("malformed result data produces a recoverable error and concurrent runs are rejected", async () => {
  const { client, workers, files, ready } = fixture(async () => { throw new Error("Invalid Arrow IPC"); });
  client.load(files); await ready(workers[0]);
  assert.equal(client.run("FROM part"), true);
  assert.equal(client.run("FROM orders"), false);
  await workers[0].emit({ type: "result-batch", requestId: client.requestId, sequence: 0, buffer: new ArrayBuffer(0) });
  assert.equal(client.state.phase, "ready");
  assert.match(client.state.error, /Invalid Arrow IPC/);
});

test("directory paths survive File structured cloning as explicit message metadata", () => {
  const { client, workers } = fixture();
  const file = new File([new Uint8Array([1, 2])], "part.parquet");
  Object.defineProperty(file, "webkitRelativePath", { value: "tpch/part/part.parquet" });
  client.load([file]);
  const cloned = structuredClone(workers[0].messages[0]);
  assert.equal(cloned.directoryName, "tpch");
  assert.equal(cloned.entries[0].path, "part/part.parquet");
  assert.equal(cloned.entries[0].file.size, 2);
});

test("results retain their exact source and Arrow references across same-name directory selections", async () => {
  const table = { decoded: true }, buffer = new ArrayBuffer(8);
  const { client, workers, files, ready } = fixture(async () => table);
  client.load(files); await ready(workers[0]);
  const source = client.state.dataset;
  client.run("FROM part");
  await complete(workers[0], client, buffer);
  const result = client.state.result;
  assert.equal(result.source, source);
  assert.equal(result.byteLength, buffer.byteLength);
  assert.equal(result.table, table);
  client.load(files); await ready(workers[1]);
  assert.equal(client.state.dataset.name, source.name);
  assert.notEqual(client.state.dataset.id, source.id);
  assert.equal(result.source, source);
  assert.equal(result.byteLength, buffer.byteLength);
  client.cancel();
  client.load(files, source.id); await ready(workers[2]);
  assert.equal(client.state.dataset.id, source.id);
});
