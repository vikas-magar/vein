import assert from "node:assert/strict";
import test from "node:test";
import { Documents } from "../../components/workbench/workspace/documents.ts";
import { resultCaption, resultState } from "../../components/workbench/results/lifecycle.ts";

const result = (query, rows = 0) => ({ byteLength: 8, table: { numRows: rows }, query, source: null });

test("never-run and released states differ, and release retains no table or result buffer", () => {
  const documents = new Documents();
  const id = documents.open("query", "FROM original");
  const original = documents.get(id);
  documents.release(id, "manual");
  assert.equal(documents.get(id), original);
  assert.equal(resultState(original), "not-run");
  documents.complete(id, result("FROM original"));
  assert.equal(resultState(documents.get(id)), "completed");
  documents.release(id, "manual");
  const released = documents.get(id);
  assert.equal(resultState(released), "released");
  assert.equal(released.result, null);
  assert.equal(released.releasedResult.reason, "manual");
  assert.equal("table" in released.releasedResult, false);
  assert.equal("buffer" in released.releasedResult, false);
  documents.edit(id, "FROM edited");
  assert.equal(documents.get(id).releasedResult.query, "FROM original");
});

test("memory eviction records the reason and a successful rerun replaces the released summary", () => {
  const documents = new Documents(8);
  const first = documents.open("first", "FROM first"), second = documents.open("second", "FROM second");
  documents.complete(first, result("FROM first", 12));
  documents.complete(second, result("FROM second"));
  assert.equal(documents.get(first).releasedResult.reason, "memory-budget");
  assert.equal(documents.get(first).releasedResult.rows, 12);
  const rerun = result("FROM first", 13);
  documents.complete(first, rerun);
  assert.equal(documents.get(first).releasedResult, null);
  assert.equal(documents.get(first).result, rerun);
  assert.equal(resultState(documents.get(first)), "completed");
});

test("failed and stopped reruns retain older rows without reporting success", () => {
  const documents = new Documents();
  const id = documents.open("query", "FROM original");
  const retained = result("FROM original", 3);
  documents.complete(id, retained);
  for (const execution of ["running", "failed", "stopped"]) {
    documents.patch(id, { execution });
    assert.equal(resultState(documents.get(id)), execution);
    assert.equal(documents.get(id).result, retained);
    assert.match(resultCaption(documents.get(id)), /previous result retained/);
  }
});
