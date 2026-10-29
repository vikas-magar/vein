import assert from "node:assert/strict";
import test from "node:test";
import { EditorState } from "@codemirror/state";
import { createStatementIndex, updateStatementIndex } from "../../components/workbench/query/statement-index.ts";
import { queryStatements } from "../../components/workbench/query/statements.ts";

const statements = index => index.segments.filter(segment => segment.statement)
  .map((segment, number) => ({ ...segment.statement, number: number + 1 }));

test("editing one statement scans that segment and preserves other control identities", () => {
  const text = Array.from({ length: 100 }, (_, i) => `FROM table_${i} |> LIMIT 1;`).join("\n");
  const state = EditorState.create({ doc: text });
  const initial = createStatementIndex(state.doc);
  const from = text.indexOf("table_50") + 6;
  const transaction = state.update({ changes: { from, insert: "new_" } });
  const next = updateStatementIndex(initial, transaction);
  assert.ok(next.scannedCharacters < 50);
  assert.deepEqual(next.segments.map(segment => segment.id), initial.segments.map(segment => segment.id));
  assert.deepEqual(statements(next), queryStatements(transaction.state.doc.toString()));
  assert.equal(next.segments[0], initial.segments[0]);
});

test("delimiter, quote, nested comment and parenthesis edits match complete lexical scanning", () => {
  const originals = ["FROM a;\nFROM b;\nFROM c", "FROM a |> WHERE x = 'a;b'; FROM b;",
    "/* outer /* nested */ */ FROM a; -- comment\nFROM b", "FROM a |> WHERE EXISTS (FROM b); FROM c", "", ";;"];
  const inserts = ["'", '"', "/*", "*/", ";", "(", ")", "--", "\n", "x", ""];
  for (const text of originals) for (let from = 0; from <= text.length; from++) {
    for (const insert of inserts) {
      const state = EditorState.create({ doc: text });
      const transaction = state.update({ changes: { from, to: Math.min(text.length, from + 2), insert } });
      const next = updateStatementIndex(createStatementIndex(state.doc), transaction);
      assert.deepEqual(statements(next), queryStatements(transaction.state.doc.toString()), `${JSON.stringify(text)} at ${from}: ${insert}`);
      assert.equal(new Set(next.segments.map(segment => segment.id)).size, next.segments.length);
    }
  }
});

test("multiple changes and undo preserve boundary correctness", () => {
  const state = EditorState.create({ doc: "FROM a; FROM b; FROM c;" });
  const index = createStatementIndex(state.doc);
  const transaction = state.update({ changes: [{ from: 5, to: 6, insert: "'a;" }, { from: 19, to: 20, insert: "'c" }] });
  const changed = updateStatementIndex(index, transaction);
  assert.deepEqual(statements(changed), queryStatements(transaction.state.doc.toString()));
  const undo = transaction.state.update({ changes: transaction.changes.invert(state.doc) });
  assert.deepEqual(statements(updateStatementIndex(changed, undo)), queryStatements(state.doc.toString()));
});

test("successive lexical edits never leave stale statements or duplicate identities", () => {
  let state = EditorState.create({ doc: "FROM a; FROM b; FROM c;" });
  let index = createStatementIndex(state.doc);
  let seed = 17;
  const next = () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0);
  const fragments = ["'", '"', ";", "/*", "*/", "--", "\n", "(", ")", "FROM x", ""];
  for (let step = 0; step < 1500; step++) {
    const from = next() % (state.doc.length + 1);
    const transaction = state.update({ changes: { from, to: Math.min(state.doc.length, from + next() % 5),
      insert: fragments[next() % fragments.length] } });
    index = updateStatementIndex(index, transaction); state = transaction.state;
    assert.deepEqual(statements(index), queryStatements(state.doc.toString()), `edit ${step}`);
    assert.equal(new Set(index.segments.map(segment => segment.id)).size, index.segments.length);
  }
});
