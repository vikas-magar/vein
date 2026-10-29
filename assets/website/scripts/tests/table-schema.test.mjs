import assert from "node:assert/strict";
import test from "node:test";
import { inspectTable, tableQuery } from "../../components/workbench/explorer/schema.ts";

test("table inspection preserves metadata references and quotes keyword and unusual names", async () => {
  const columns = [{ name: "amount", data_type: "Int64", qualifier: "order" }];
  const calls = [];
  const provider = { getSnapshot: () => ({ phase: "ready", dataset: { id: "selection" } }),
    inspect: async (...args) => { calls.push(args); return { columns, functions: [], diagnostics: [] }; } };
  const schema = await inspectTable(provider, "order", "selection");
  assert.equal(schema.columns, columns);
  assert.deepEqual(calls, [['FROM "order"', 'FROM "order"']]);
  assert.equal(tableQuery('odd"name'), 'FROM "odd""name"');
});

test("busy and stale sources do not trigger metadata work or publish old schemas", async () => {
  let source = "old", phase = "running", resolve;
  let calls = 0;
  const provider = { getSnapshot: () => ({ phase, dataset: { id: source } }),
    inspect: () => { calls++; return new Promise(done => { resolve = done; }); } };
  assert.match((await inspectTable(provider, "table", "old")).error, /Wait/);
  assert.equal(calls, 0);
  assert.equal((await inspectTable(provider, "table", "other")).stale, true);
  assert.equal(calls, 0);
  phase = "ready";
  const pending = inspectTable(provider, "table", "old");
  source = "new";
  resolve({ columns: [{ name: "old_column" }], diagnostics: [], functions: [] });
  const schema = await pending;
  assert.equal(schema.stale, true);
  assert.deepEqual(schema.columns, []);
});

test("binding errors and unavailable inspection remain explicit instead of inventing columns", async () => {
  const provider = { getSnapshot: () => ({ phase: "ready", dataset: { id: "source" } }), inspect: async () => null };
  assert.match((await inspectTable(provider, "table", "source")).error, /temporarily unavailable/);
  provider.inspect = async () => ({ columns: [], functions: [], diagnostics: [{ message: "Missing table" }] });
  assert.equal((await inspectTable(provider, "table", "source")).error, "Missing table");
});
