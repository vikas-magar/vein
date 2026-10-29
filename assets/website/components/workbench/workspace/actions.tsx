import { useCallback, useEffect, useMemo, useRef } from "react";
import type { WorkspaceSession } from "./session";
import type { WorkspaceLayout } from "../layout/controller";
import { useOutputNavigation } from "./output-navigation";

/** Shared directory/run flow for the full workbench and inline documentation editors. */
export function useWorkspaceActions(session: WorkspaceSession, layout: WorkspaceLayout) {
  useOutputNavigation(session, layout);
  const input = useRef<HTMLInputElement>(null);
  const pending = useRef<{ id: string; query?: string; statementNumber?: number } | null>(null);
  const subscription = useRef<(() => void) | null>(null);
  const openDirectory = useCallback(() => { pending.current = null; input.current?.click(); }, []);
  const run = useCallback((id: string, query?: string, statementNumber?: number) => {
    if (!session.documents.get(id)?.text.trim() || ["running", "loading"].includes(session.snapshot.phase)) return;
    layout.selectDocument(id);
    layout.showResults();
    if (!session.run(id, query, statementNumber)) { pending.current = { id, query, statementNumber }; input.current?.click(); }
  }, [session, layout]);
  useEffect(() => {
    const element = input.current;
    const cancel = () => { pending.current = null; };
    element?.addEventListener("cancel", cancel);
    return () => { element?.removeEventListener("cancel", cancel); subscription.current?.(); };
  }, []);
  const onChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(event.currentTarget.files ?? []); event.currentTarget.value = "";
    const requested = pending.current; pending.current = null;
    if (!files.length) return;
    subscription.current?.(); session.load(files);
    if (requested) subscription.current = session.subscribe(() => {
      if (session.snapshot.phase === "ready") { subscription.current?.(); subscription.current = null; session.run(requested.id, requested.query, requested.statementNumber); }
      else if (session.snapshot.phase === "error") { subscription.current?.(); subscription.current = null; }
    });
  }, [session]);
  const value = useMemo(() => ({ session, layout, openDirectory, run }), [session, layout, openDirectory, run]);
  const directoryInput = <input ref={input} type="file" multiple hidden {...{ webkitdirectory: "" }}
    aria-label="Choose data directory" onChange={onChange} />;
  return { value, directoryInput, run };
}
