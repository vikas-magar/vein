import { syntaxTree } from "@codemirror/language";
import type { EditorState } from "@codemirror/state";
import { queryAt } from "./statements";

export function inLiteral(state: EditorState, position: number): boolean {
  return /string|comment/i.test(syntaxTree(state).resolveInner(position, -1).name);
}

/** Only complete stages are sent to the binder as the input schema for an unfinished stage. */
export function inspectionContext(state: EditorState, position: number) {
  const text = state.doc.toString();
  const statement = queryAt(text, position);
  if (!statement) return null;
  let boundary = -1;
  for (let at = text.indexOf("|>", statement.from); at >= 0 && at < position; at = text.indexOf("|>", at + 2)) {
    if (!inLiteral(state, at + 1)) boundary = at;
  }
  return { statement, prefix: boundary < 0 ? "" : text.slice(statement.from, boundary).trim(),
    before: text.slice(statement.from, position) };
}

export function quoteIdentifier(name: string, alwaysQuote = false) {
  return !alwaysQuote && /^[\p{L}_][\p{L}\p{N}_$]*$/u.test(name) ? name : `"${name.replaceAll('"', '""')}"`;
}
