import assert from "node:assert/strict";
import test from "node:test";
import { tableFromArrays, tableFromIPC, tableToIPC } from "apache-arrow";
import { ResultDelivery } from "../../public/worker/result-delivery.mjs";
import { ResultCollector } from "../../components/workbench/engine/result-collector.mjs";
import { assembleArrowTables } from "../../components/workbench/results/assemble.mjs";

test("delivery pauses after every transfer and ignores stale or out-of-sequence acknowledgements", async () => {
  const messages = [];
  const delivery = new ResultDelivery((data, transfer) => {
    messages.push(structuredClone(data, { transfer }));
  }, () => performance.now(), 16);
  let produced = 0;
  const engine = { async query(query, consume, limit) {
    assert.equal(limit, 16);
    for (let i = 0; i < 3; i++) { produced++; await consume(new Uint8Array([i])); }
  } };
  const running = delivery.run(engine, { requestId: 7, query: "q" });
  assert.equal(produced, 1);
  delivery.acknowledge({ type: "result-ack", requestId: 6, sequence: 0, accepted: true });
  delivery.acknowledge({ type: "result-ack", requestId: 7, sequence: 1, accepted: true });
  await Promise.resolve(); assert.equal(produced, 1);
  for (let sequence = 0; sequence < 3; sequence++) {
    assert.equal(messages.length, sequence + 1);
    assert.deepEqual([...new Uint8Array(messages[sequence].buffer)], [sequence]);
    delivery.acknowledge({ type: "result-ack", requestId: 7, sequence, accepted: true });
    await new Promise(resolve => setImmediate(resolve));
  }
  const metrics = await running;
  assert.equal(metrics.transport.packets, 3);
  assert.equal(metrics.transport.maxPacketBytes, 1);
  assert.equal(delivery.pending, null);
});

test("consumer rejection and oversized packets release transport state", async () => {
  let message;
  const delivery = new ResultDelivery(data => { message = data; }, () => 0, 4);
  const pending = delivery.run({ query: async (_, consume) => consume(new Uint8Array([1])) }, { requestId: 1 });
  delivery.acknowledge({ type: "result-ack", requestId: 1, sequence: message.sequence, accepted: false, message: "decode failed" });
  await assert.rejects(pending, /decode failed/);
  assert.equal(delivery.pending, null);
  await assert.rejects(delivery.run({ query: async (_, consume) => consume(new Uint8Array(5)) }, { requestId: 2 }), /packet limit/);
  assert.equal(delivery.pending, null);
});

test("Arrow packet assembly preserves buffers, order, NULLs and schema; release drops owners", async () => {
  const tables = [], buffers = [];
  const collector = new ResultCollector(async buffer => {
    const table = tableFromIPC(new Uint8Array(buffer)); tables.push(table); return table;
  }, assembleArrowTables);
  for (const [sequence, values] of [[1, null], [3, 4]].entries()) {
    const ipc = tableToIPC(tableFromArrays({ id: values }));
    const buffer = ipc.buffer.slice(ipc.byteOffset, ipc.byteOffset + ipc.byteLength); buffers.push(buffer);
    await collector.append({ sequence, buffer });
  }
  const byteLength = buffers.reduce((sum, buffer) => sum + buffer.byteLength, 0);
  const result = collector.finish({ packets: 2, byteLength });
  assert.deepEqual([...result.table.getChildAt(0)], [1, null, 3, 4]);
  assert.equal(result.byteLength, byteLength);
  assert.equal(result.table.batches[0].data.children[0].values.buffer, tables[0].batches[0].data.children[0].values.buffer);
  assert.equal(result.table.batches[1].data.children[0].values.buffer, tables[1].batches[0].data.children[0].values.buffer);
  assert.equal(result.table.getChildAt(0).data[0].values.buffer, buffers[0]);
  assert.throws(() => collector.finish({ packets: 1, byteLength }), /Incomplete/);
  await assert.rejects(collector.append({ sequence: 4, buffer: new ArrayBuffer(0) }), /sequence/);
  collector.release();
  assert.equal(collector.tables.length, 0);
  assert.equal(collector.byteLength, 0);
});
