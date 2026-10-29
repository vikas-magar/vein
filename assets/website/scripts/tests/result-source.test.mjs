import assert from "node:assert/strict";
import test from "node:test";
import { resultSourceState } from "../../components/workbench/results/source.ts";

test("source names do not prove identity, and reconnecting the retained selection stays current", () => {
  const source = { id: "first-selection", name: "warehouse" };
  assert.equal(resultSourceState(source, { id: "other-selection", name: "warehouse" }), "previous");
  assert.equal(resultSourceState(source, { ...source }), "current");
  assert.equal(resultSourceState(source, null), "disconnected");
  assert.equal(resultSourceState(null, source), "unknown");
});
