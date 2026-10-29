import dynamic from "next/dynamic";
import { useDocument, useWorkspace } from "../workspace/hooks";
import EditorBoundary from "../editor-boundary";
import FallbackEditor from "./fallback";

const Editor = dynamic(() => import("./editor"), { ssr: false,
  loading: () => <div className="ide-empty">Starting editor…</div> });

export default function QueryPanel({ documentId }: { documentId: string }) {
  const document = useDocument(documentId);
  const { session } = useWorkspace();
  if (!document) return <div className="ide-empty">This query was closed.</div>;
  return <section className="ide-query" aria-label={document.name}>
    <EditorBoundary key={documentId} fallback={<FallbackEditor documentId={documentId} />}
      onError={() => session.documents.editorStates.delete(documentId)}>
      <Editor documentId={documentId} />
    </EditorBoundary>
    <div className="ide-editor-footer"><span>Vein Pipe</span><span>{document.text.split("\n").length} lines</span>
      <span className="ide-toolbar-spacer" /><span>UTF-8</span></div>
  </section>;
}
