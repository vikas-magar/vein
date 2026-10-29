import assert from "node:assert/strict";
import test from "node:test";
import { queryAt, queryStatements } from "../../components/workbench/query/statements.ts";

test("multiline pipelines are one query, semicolons separate statements", () => {
  const text = "FROM smoke\n|> SELECT id;\nFROM smoke\n|> LIMIT 2\n\n";
  const queries = queryStatements(text);
  assert.deepEqual(queries.map(value => value.query), ["FROM smoke\n|> SELECT id", "FROM smoke\n|> LIMIT 2"]);
  assert.equal(queryAt(text, text.indexOf("LIMIT")).query, queries[1].query);
  assert.equal(queryAt(text, text.length).number, 2);
});

test("quoted delimiters, escaped quotes and comments do not create controls", () => {
  const text = "-- ; ignored\nFROM smoke |> WHERE name = 'it''s;fine' AND \"a;b\" = 1;\n/* outer ; /* inner ; */ */\nFROM smoke -- ; ignored\n|> LIMIT 2; -- only a comment ;";
  const queries = queryStatements(text);
  assert.equal(queries.length, 2);
  assert.match(queries[0].query, /'it''s;fine'/);
  assert.equal(queries[1].query, "FROM smoke -- ; ignored\n|> LIMIT 2");
  assert.deepEqual(queryStatements("; -- comment\n/* ; */ ;\n"), []);
  assert.deepEqual(queryStatements(""), []);
});
