import { useMemo, useState } from "react";
import { useDocument, useWorkspace } from "../workspace/hooks";
import QueryControls from "./controls";
import { queryAt } from "./statements";

/** Plain-text editing shares statement selection, execution controls and document ownership. */
export default function FallbackEditor({ documentId }: { documentId: string }) {
  const document = useDocument(documentId);
  const { session, run } = useWorkspace();
  const text = document?.text ?? "";
  const [position, setPosition] = useState(text.length);
  const selected = useMemo(() => queryAt(text, Math.min(position, text.length)), [text, position]);
  const executing = document?.execution === "running";
  const query = executing ? document.submitted : selected?.query ?? "";
  const number = executing ? document.submittedStatementNumber : selected?.number ?? null;
  return <div className="editor-fallback">
    <p role="note">Enhanced editor unavailable. Plain-text editing and query execution remain available.</p>
    <textarea aria-label="Vein Pipe query" value={text} spellCheck={false}
      onChange={event => {
        setPosition(event.currentTarget.selectionStart);
        session.documents.edit(documentId, event.currentTarget.value);
      }}
      onSelect={event => setPosition(event.currentTarget.selectionStart)}
      onKeyDown={event => {
        if (event.key !== "Enter" || !(event.ctrlKey || event.metaKey)) return;
        event.preventDefault(); event.stopPropagation();
        const statement = queryAt(text, event.currentTarget.selectionStart);
        if (statement) run(documentId, statement.query, statement.number);
      }} />
    <div className="ide-fallback-execution">
      <span>{number ? `Statement ${number}` : query ? "Executed statement" : "Enter a query"}</span>
      <QueryControls documentId={documentId} query={query} statementNumber={number} />
    </div>
    <small>Run uses the statement at the cursor · Ctrl / ⌘ Enter</small>
  </div>;
}
