import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";
export { language } from "./language";

export const syntax = syntaxHighlighting(HighlightStyle.define([
  { tag: tags.keyword, color: "var(--syntax-keyword)" },
  { tag: tags.string, color: "var(--syntax-string)" },
  { tag: [tags.number, tags.bool, tags.null], color: "var(--syntax-number)" },
  { tag: tags.function(tags.variableName), color: "var(--syntax-function)" },
  { tag: tags.comment, color: "var(--subtle)" },
  { tag: [tags.operator, tags.punctuation], color: "var(--syntax-operator)" },
]));
export const theme = EditorView.theme({
  "&": { height: "100%", background: "var(--page)", color: "var(--ink)", fontSize: "var(--editor-font-size, 13px)" },
  ".cm-scroller": { overflow: "auto", fontFamily: "var(--mono)", lineHeight: "1.8" },
  ".cm-content": { padding: "16px 0", caretColor: "var(--accent-bright)" },
  ".cm-line": { padding: "0 20px 0 12px" },
  ".cm-gutters": { background: "var(--page)", color: "var(--subtle)", border: "none", minWidth: "46px" },
  ".cm-activeLine, .cm-activeLineGutter": { background: "#ffffff04" },
  ".cm-cursor": { borderLeftColor: "var(--accent-bright)" },
  ".cm-selectionBackground": { background: "var(--accent-soft) !important" },
  ".cm-tooltip": { background: "var(--panel-raised)", border: "1px solid var(--line)" },
  ".cm-panel.cm-panel-lint": { background: "var(--panel)", color: "var(--ink)", borderTop: "1px solid var(--line)", maxHeight: "110px", fontSize: "11px" },
  ".cm-diagnosticSource": { color: "var(--muted)" },
}, { dark: true });
