import type { CompletionContext, CompletionResult } from "@codemirror/autocomplete";
import { EditorView } from "@codemirror/view";
import { inLiteral } from "./inspection-context";

const stages = ["JOIN", "LEFT JOIN", "WHERE", "SELECT", "EXTEND", "AGGREGATE", "ORDER", "LIMIT"];
const keywords = ["FROM", ...stages, "GROUP BY"];

/** Stage completions insert syntax, while expression completions remain plain keywords. */
export function pipeCompletion(context: CompletionContext): CompletionResult | null {
  if (inLiteral(context.state, context.pos)) return null;
  const word = context.matchBefore(/\w*/);
  if (!word || (!context.explicit && word.from === word.to)) return null;
  const line = context.state.doc.lineAt(context.pos);
  const prefix = context.state.sliceDoc(line.from, word.from);
  const stageStart = line.number > 1 && /^\s*$/.test(prefix);
  const piped = /^\s*\|>\s*$/.test(prefix);
  return {
    from: word.from,
    options: (stageStart || piped ? stages : keywords).map(keyword => ({
      label: keyword,
      displayLabel: stageStart ? `|> ${keyword}` : keyword,
      apply: stageStart ? `|> ${keyword} ` : `${keyword} `,
      type: "keyword",
    })),
    validFor: /^\w*$/,
  };
}

/** Complete a typed pipe without touching pasted text, strings, comments or selections. */
export const pipeInput = EditorView.inputHandler.of((view, from, to, text) => {
  if (text !== "|" || from !== to || inLiteral(view.state, from)) return false;
  if (view.state.sliceDoc(from, from + 1) === ">") return false;
  view.dispatch({ changes: { from, to, insert: "|> " }, selection: { anchor: from + 3 },
    userEvent: "input.type" });
  return true;
});
