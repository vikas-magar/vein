import assert from "node:assert/strict";
import test from "node:test";
import { StreamLanguage } from "@codemirror/language";
import { highlightTree, tagHighlighter, tags } from "@lezer/highlight";
import { veinPipe } from "../../components/workbench/pipe-language.mjs";

const highlighter = tagHighlighter([
  { tag: tags.keyword, class: "keyword" },
  { tag: tags.operator, class: "operator" },
  { tag: tags.function(tags.variableName), class: "function" },
  { tag: tags.string, class: "string" },
  { tag: tags.number, class: "number" },
  { tag: tags.comment, class: "comment" },
]);

test("CodeMirror parses and highlights an actual Vein Pipe query", () => {
  const query = "FROM lineitem\n|> WHERE l_shipmode = 'MAIL'\n|> AGGREGATE sum(l_quantity) AS total\n|> LIMIT 10 -- preview";
  const tree = StreamLanguage.define(veinPipe).parser.parse(query);
  const tokens = [];
  highlightTree(tree, highlighter, (from, to, style) => tokens.push([query.slice(from, to), style]));
  for (const token of [["FROM", "keyword"], ["|>", "operator"], ["'MAIL'", "string"],
    ["sum", "function"], ["10", "number"], ["-- preview", "comment"]]) {
    assert.ok(tokens.some(([text, style]) => text === token[0] && style === token[1]), `${token} missing: ${JSON.stringify(tokens)}`);
  }
});

test("quoted strings and decimals do not consume following Pipe stages", () => {
  const query = "FROM part |> WHERE p_name = 'it''s brass' AND p_size > 1.5 |> SELECT p_name";
  const tokens = [];
  highlightTree(StreamLanguage.define(veinPipe).parser.parse(query), highlighter,
    (from, to, style) => tokens.push([query.slice(from, to), style]));
  assert.ok(tokens.some(([text, style]) => text === "'it''s brass'" && style === "string"));
  assert.ok(tokens.some(([text, style]) => text === "1.5" && style === "number"));
  assert.ok(tokens.some(([text, style]) => text === "SELECT" && style === "keyword"));
});
