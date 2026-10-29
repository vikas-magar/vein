import assert from "node:assert/strict";
import test from "node:test";
import { tableFromArrays, tableToIPC } from "apache-arrow";
import { WorkspaceSession } from "../../components/workbench/workspace/session.ts";

test("late results stay attached to the submitted document and stop reconnects without selecting files again", async () => {
  const original = globalThis.Worker;
  const workers = [];
  globalThis.Worker = class {
    messages = [];
    constructor() { workers.push(this); }
    postMessage(message) { this.messages.push(message); }
    terminate() { this.stopped = true; }
    emit(data) { return this.onmessage?.({ data }); }
  };
  try {
    const session = new WorkspaceSession();
    const first = session.documents.open("first", "FROM first");
    assert.equal(session.documents.get(first).execution, "idle");
    assert.equal(session.run(first), false);
    assert.equal(session.documents.get(first).execution, "idle");
    assert.equal(session.documents.get(first).submitted, "");
    session.load([{ name: "first.parquet", webkitRelativePath: "root/first/first.parquet", size: 20 }]);
    await workers[0].emit({ type: "ready", requestId: workers[0].messages[0].requestId,
      fileCount: 1, totalBytes: 20, directoryName: "root", tableNames: ["first"] });
    const source = session.snapshot.dataset;
    assert.equal(session.run(first), true);
    assert.equal(session.documents.get(first).execution, "running");
    session.documents.rename(first, "renamed");
    session.documents.edit(first, "FROM edited_while_running");
    const second = session.documents.open("second", "FROM second");
    const ipc = tableToIPC(tableFromArrays({ value: [42] }));
    const buffer = ipc.buffer.slice(ipc.byteOffset, ipc.byteOffset + ipc.byteLength);
    const requestId = workers[0].messages.at(-1).requestId;
    await workers[0].emit({ type: "result-batch", requestId, sequence: 0, buffer });
    await workers[0].emit({ type: "result", requestId, engineMs: 1,
      transport: { packets: 1, byteLength: buffer.byteLength } });
    assert.equal(session.documents.get(first).result.byteLength, buffer.byteLength);
    assert.equal(session.documents.get(first).execution, "completed");
    assert.equal(session.documents.get(first).result.source, source);
    assert.deepEqual(session.documents.get(first).resultOrigin, { documentName: "first", statementNumber: 1 });
    assert.equal(session.documents.get(first).result.query, "FROM first");
    assert.equal(session.documents.get(second).result, null);
    assert.equal(session.snapshot.result, null);
    session.documents.edit(first, "FROM first");
    session.run(first); session.stop();
    assert.equal(session.documents.get(first).execution, "stopped");
    assert.equal(workers[0].stopped, true);
    assert.equal(session.run(first), true);
    assert.equal(workers.length, 2);
    assert.equal(workers[1].messages[0].type, "load");
    await workers[1].emit({ type: "ready", requestId: workers[1].messages[0].requestId,
      fileCount: 1, totalBytes: 20, directoryName: "root", tableNames: ["first"] });
    assert.equal(workers[1].messages.at(-1).type, "query");
    assert.equal(workers[1].messages.at(-1).query, "FROM first");
    assert.equal(session.snapshot.dataset.id, source.id);
    await workers[1].emit({ type: "error", requestId: workers[1].messages.at(-1).requestId, message: "Unknown column" });
    assert.equal(session.documents.get(first).execution, "failed");
    assert.equal(session.documents.get(first).result.byteLength, buffer.byteLength);
    assert.equal(session.run(first), true);
    session.load([{ name: "first.parquet", webkitRelativePath: "root/first/first.parquet", size: 20 }]);
    assert.equal(session.documents.get(first).execution, "stopped");
    assert.match(session.documents.get(first).error, /data directory changed/);
    assert.equal(session.documents.get(first).result.byteLength, buffer.byteLength);
    await workers[2].emit({ type: "ready", requestId: workers[2].messages[0].requestId,
      fileCount: 1, totalBytes: 20, directoryName: "root", tableNames: ["first"] });
    assert.notEqual(session.snapshot.dataset.id, source.id);
  } finally { globalThis.Worker = original; }
});
