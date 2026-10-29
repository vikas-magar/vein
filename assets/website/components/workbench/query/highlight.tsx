import { classHighlighter, highlightTree } from "@lezer/highlight";
import { language } from "./language";
import type { ReactNode } from "react";

/** Server-rendered syntax colors; documentation does not download an editor to show code. */
export default function HighlightedCode({ query }: { query: string }) {
  const parts: ReactNode[] = [];
  let cursor = 0;
  highlightTree(language.parser.parse(query), classHighlighter, (from, to, classes) => {
    if (cursor < from) parts.push(query.slice(cursor, from));
    parts.push(<span key={from} className={classes}>{query.slice(from, to)}</span>);
    cursor = to;
  });
  if (cursor < query.length) parts.push(query.slice(cursor));
  return <pre><code>{parts}</code></pre>;
}
