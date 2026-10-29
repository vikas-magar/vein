import assert from "node:assert/strict";
import test from "node:test";
import { EditorState } from "@codemirror/state";
import { language } from "../../components/workbench/query/language.ts";
import { inspectionContext, quoteIdentifier } from "../../components/workbench/query/inspection-context.ts";

test("completion binds the complete input pipeline and ignores pipes inside literals", () => {
  const query = "FROM smoke AS s |> WHERE s.name = '|>' |> EXTEND s.id + 1 AS next_id |> SELECT ne";
  const state = EditorState.create({ doc: query, extensions: [language] });
  const context = inspectionContext(state, query.length);
  assert.equal(context.prefix, "FROM smoke AS s |> WHERE s.name = '|>' |> EXTEND s.id + 1 AS next_id");
  assert.equal(context.statement.query, query);
});

test("completion uses the current semicolon-separated statement and quotes unusual identifiers", () => {
  const query = "FROM first; FROM second |> WHERE va";
  const context = inspectionContext(EditorState.create({ doc: query, extensions: [language] }), query.length);
  assert.equal(context.prefix, "FROM second");
  assert.equal(quoteIdentifier('a"b'), '"a""b"');
  assert.equal(quoteIdentifier("normal_name"), "normal_name");
});
