import { useEffect } from "react";
import type { WorkspaceSession } from "./session";
import type { WorkspaceLayout } from "../layout/controller";

/** Reveal completion only for the query the user is still viewing. Retain no result payloads. */
export function useOutputNavigation(session: WorkspaceSession, layout: WorkspaceLayout) {
  useEffect(() => {
    let id = layout.getActive();
    let running = session.documents.get(id)?.execution === "running";
    return session.documents.subscribeChanges(() => {
      const currentId = layout.getActive();
      const document = session.documents.get(currentId);
      const completed = id === currentId && running && document && document.execution !== "running";
      id = currentId;
      running = document?.execution === "running";
      if (completed && (document.result || document.error)) layout.showResults();
    });
  }, [session, layout]);
}
