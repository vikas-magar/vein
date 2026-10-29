import assert from "node:assert/strict";
import test from "node:test";
import { tableFromArrays } from "apache-arrow";
import { Documents } from "../../components/workbench/workspace/documents.ts";
import { ArrowGridModel } from "../../components/workbench/results/arrow-model.ts";
import { WorkspaceLayout } from "../../components/workbench/layout/controller.ts";
import { saveWorkspace, restoreWorkspace } from "../../components/workbench/workspace/persistence.ts";
import { Actions } from "flexlayout-react";

test("editing one document leaves other documents and Arrow result references untouched", () => {
  const documents = new Documents();
  const first = documents.open("first", "FROM first");
  const second = documents.open("second", "FROM second");
  const table = tableFromArrays({ id: [1, 2] });
  const result = { table, byteLength: 8 };
  documents.complete(second, result);
  const stable = documents.get(second);
  let changes = 0;
  documents.subscribe(second, () => changes++);
  documents.edit(first, "FROM first |> LIMIT 1");
  assert.equal(documents.get(second), stable);
  assert.equal(documents.get(second).result, result);
  assert.equal(documents.get(second).result.table, table);
  assert.equal(changes, 0);
});

test("result budget evicts old owners without copying the latest payload", () => {
  const documents = new Documents(10);
  const a = documents.open("a", "a"), b = documents.open("b", "b");
  const latest = { byteLength: 8, table: { numRows: 1 }, query: "b", source: null };
  documents.complete(a, { byteLength: 8, table: { numRows: 1 }, query: "a", source: null });
  documents.complete(b, latest);
  assert.equal(documents.get(a).result, null);
  assert.equal(documents.get(b).result, latest);
  documents.editorStates.set(b, { selection: 3 });
  documents.close(b);
  assert.equal(documents.editorStates.has(b), false);
  assert.equal(documents.get(b), undefined);
});

test("opening an existing example preserves its edited draft and selects its tab", () => {
  const documents = new Documents();
  const layout = new WorkspaceLayout(documents);
  layout.open("Q1", "FROM original", "Q1");
  documents.edit("Q1", "FROM edited");
  layout.open("Q2", "FROM other", "Q2");
  layout.open("Q1", "FROM original", "Q1");
  assert.equal(layout.getActive(), "Q1");
  assert.equal(documents.get("Q1").text, "FROM edited");
  assert.equal(documents.list().length, 2);
  const action = Actions.selectTab("document:Q2");
  layout.model.doAction(action); layout.changed(layout.model, action);
  assert.equal(layout.getActive(), "Q2");
});

test("draft persistence excludes payloads and rejects corrupt storage", () => {
  const documents = new Documents();
  documents.open("q", "FROM original", "query-1");
  documents.edit("query-1", "FROM edited");
  documents.complete("query-1", { byteLength: 8, table: { sensitive: "payload" } });
  let value;
  const storage = { getItem: () => value, setItem: (_, next) => { value = next; } };
  saveWorkspace(storage, documents, { layout: { type: "row", children: [] } });
  assert.equal(value.includes("payload"), false);
  const restored = new Documents();
  restoreWorkspace(storage, restored);
  assert.equal(restored.get("query-1").text, "FROM edited");
  assert.equal(restored.get("query-1").result, null);
  assert.notEqual(restored.open("new", ""), "query-1");
  value = "invalid";
  assert.doesNotThrow(() => restoreWorkspace(storage, new Documents()));
});

test("Arrow grid reads cells by index without converting complete tables", () => {
  const table = tableFromArrays({ number: [1, 2], label: ["one", null] });
  table.toArray = () => { throw new Error("whole-table conversion is forbidden"); };
  const model = new ArrowGridModel(table, 2);
  assert.equal(model.table, table);
  assert.equal(model.text(0, 0), "1");
  assert.equal(model.text(1, 1), "NULL");
  assert.equal(model.isNull(1, 1), true);
});

test("restoring a layout removes orphan tabs without creating missing documents", () => {
  const original = new WorkspaceLayout(new Documents());
  original.open("lost", "FROM lost", "lost");
  const documents = new Documents();
  const restored = new WorkspaceLayout(documents, original.model.toJson());
  assert.equal(restored.model.getNodeById("document:lost"), undefined);
  assert.equal(restored.getActive(), "");
  assert.equal(documents.list().length, 0);
});

test("restoring a layout uses the selected query rather than the last document visited", () => {
  const documents = new Documents();
  const layout = new WorkspaceLayout(documents);
  layout.open("first", "FROM first", "first");
  layout.open("second", "FROM second", "second");
  layout.open("first", "FROM first", "first");
  const restored = new WorkspaceLayout(documents, layout.model.toJson());
  assert.equal(restored.getActive(), "first");
});

test("revealing Results preserves query ownership when another output tab was selected", () => {
  const documents = new Documents();
  const layout = new WorkspaceLayout(documents);
  layout.open("first", "FROM first", "first");
  let results, messages;
  layout.model.visitNodes(node => {
    if (node.getComponent?.() === "results") results = node;
    if (node.getComponent?.() === "messages") messages = node;
  });
  layout.model.doAction(Actions.selectTab(messages.getId()));
  layout.showResults();
  assert.equal(results.getParent().getSelectedNode(), results);
  assert.equal(layout.getActive(), "first");
});
