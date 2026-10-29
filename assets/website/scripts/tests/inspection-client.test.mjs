import assert from "node:assert/strict";
import test from "node:test";
import { InspectionClient } from "../../components/workbench/engine/inspection-client.mjs";

const inspection = { columns: [], functions: [], diagnostics: [] };

test("inspection coalesces identical work, bounds queued requests and caches metadata only", async () => {
  const messages = [];
  const client = new InspectionClient(() => ({ postMessage: message => messages.push(message) }), () => true);
  const first = client.inspect("first");
  assert.equal(client.inspect("first"), first);
  const dropped = client.inspect("old draft");
  const last = client.inspect("new draft");
  assert.equal(await dropped, null);
  assert.equal(messages.length, 1);
  client.receive({ type: "inspection", requestId: messages[0].requestId, inspection });
  assert.equal(await first, inspection);
  assert.equal(messages.length, 2);
  client.receive({ type: "inspection", requestId: messages[1].requestId, inspection });
  assert.equal(await last, inspection);
  assert.equal(await client.inspect("new draft"), inspection);
  assert.equal(messages.length, 2);
  client.cancel();
});

test("cancellation, unavailable engines and late replies never affect the execution channel", async () => {
  let ready = true;
  const messages = [];
  const client = new InspectionClient(() => ({ postMessage: message => messages.push(message) }), () => ready);
  const pending = client.inspect("first");
  client.cancel();
  assert.equal(await pending, null);
  assert.equal(client.receive({ type: "result", requestId: 2 }), false);
  assert.equal(client.receive({ type: "inspection", requestId: messages[0].requestId, inspection }), true);
  ready = false;
  assert.equal(await client.inspect("next"), null);
  assert.equal(messages.length, 1);
});
