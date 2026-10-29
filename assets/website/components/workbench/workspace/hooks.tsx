import { createContext, useContext, useSyncExternalStore } from "react";
import type { WorkspaceSession } from "./session";
import type { WorkspaceLayout } from "../layout/controller";
import type { QueryDocument } from "./contracts";

export interface WorkspaceActions {
  session: WorkspaceSession; layout: WorkspaceLayout;
  openDirectory: () => void; run: (id: string, query?: string, statementNumber?: number) => void;
}
export const WorkspaceContext = createContext<WorkspaceActions | null>(null);
export function useWorkspace() {
  const value = useContext(WorkspaceContext);
  if (!value) throw new Error("Workspace provider is missing");
  return value;
}
export function useEngineSnapshot() {
  const { session } = useWorkspace();
  return useSyncExternalStore(session.subscribe, session.getSnapshot, session.getSnapshot);
}
export function useDocument(id: string) {
  const { session } = useWorkspace();
  return useSyncExternalStore(callback => session.documents.subscribe(id, callback),
    () => session.documents.get(id), () => session.documents.get(id));
}
/** Select a stable value so unrelated text edits do not invalidate execution controls. */
export function useDocumentValue<T>(id: string, select: (document: QueryDocument | undefined) => T) {
  const { session } = useWorkspace();
  return useSyncExternalStore(callback => session.documents.subscribe(id, callback),
    () => select(session.documents.get(id)), () => select(session.documents.get(id)));
}
export function useActiveDocument() {
  const { layout } = useWorkspace();
  return useSyncExternalStore(layout.subscribe, layout.getActive, layout.getActive);
}
