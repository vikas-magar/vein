import { linter, lintGutter } from "@codemirror/lint";
import type { WorkspaceSession } from "../workspace/session";
import type { EngineDiagnostic, Inspection } from "./inspection-types";
import { queryAt } from "./statements";

export function queryDiagnostics(session: WorkspaceSession, report: (errors: EngineDiagnostic[]) => void) {
  return [linter(async view => {
    if (!view.inView) return [];
    const document = view.state.doc;
    const cursor = view.state.selection.main.head;
    const statement = queryAt(document.toString(), cursor);
    if (!statement) { report([]); return []; }
    const inspection: Inspection | null = await session.inspect(statement.query);
    if (!view.inView || view.state.doc !== document || view.state.selection.main.head !== cursor) return [];
    const errors = (inspection?.diagnostics ?? []).map(error => {
      const valid = error.from !== null && error.to !== null && error.from >= 0
        && error.to >= error.from && error.to <= statement.query.length;
      return { ...error, from: valid ? statement.from + error.from! : null,
        to: valid ? statement.from + error.to! : null };
    });
    report(errors);
    return errors.filter(error => error.from !== null && error.to !== null).map(error => {
      return { from: error.from!, to: error.to!,
        severity: "error" as const, message: error.message,
        source: `Vein ${error.category}` };
    }).filter(error => error.from >= 0 && error.to >= error.from && error.to <= document.length);
   }, { delay: 500, autoPanel: false,
    needsRefresh: update => update.selectionSet }), lintGutter()];
}
