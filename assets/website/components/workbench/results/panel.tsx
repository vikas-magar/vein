import { useActiveDocument, useDocument } from "../workspace/hooks";
import Grid from "./grid";
import EmptyResult from "./empty";
import { resultState } from "./lifecycle";

export default function ResultsPanel() {
  const id = useActiveDocument();
  const document = useDocument(id);
  const result = document?.result;
  const state = resultState(document);
  return <section className="ide-results" aria-label="Query results" aria-busy={state === "running"} data-state={state}>
    {document?.error && <div className={state === "stopped" ? "ide-execution-note" : "ide-error"}
      role={state === "stopped" ? "note" : "alert"}>{document.error}</div>}
    {result ? <Grid table={result.table} /> : <EmptyResult document={document} />}
  </section>;
}
