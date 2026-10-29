"use client";

import { useEffect, useState } from "react";
import { getWorkspaceSession } from "./session";
import { WorkspaceContext } from "./hooks";
import { WorkspaceLayout } from "../layout/controller";
import WorkspaceDock from "../layout/dock";
import Header from "./header";
import Status from "./status";
import { restoreWorkspace, saveWorkspace } from "./persistence";
import { useWorkspaceActions } from "./actions";
import "flexlayout-react/style/dark.css";
import "../styles/ide.css";

export default function Workspace() {
  const [session] = useState(getWorkspaceSession);
  const [layout] = useState(() => {
    let saved;
    try { saved = restoreWorkspace(window.localStorage, session.documents); } catch { /* Storage may be disabled. */ }
    const compact = window.matchMedia("(max-width: 760px)").matches;
    try { return new WorkspaceLayout(session.documents, saved, compact); }
    catch { return new WorkspaceLayout(session.documents, undefined, compact); }
  });
  const { value, directoryInput, run } = useWorkspaceActions(session, layout);
  useEffect(() => {
    if (!layout.getActive()) layout.open("Welcome.vein", "FROM lineitem\n|> AGGREGATE count(*) AS row_count", "welcome");
    function shortcut(event: KeyboardEvent) {
      if (event.defaultPrevented || !(event.ctrlKey || event.metaKey)) return;
      if (event.key === "Enter") { event.preventDefault(); run(layout.getActive()); }
    }
    window.addEventListener("keydown", shortcut);
    return () => window.removeEventListener("keydown", shortcut);
  }, [layout, run]);
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;
    const save = () => { try { saveWorkspace(window.localStorage, session.documents, layout.model.toJson()); } catch {} };
    const schedule = () => { clearTimeout(timer); timer = setTimeout(save, 400); };
    const unsubscribe = session.documents.subscribeChanges(schedule);
    const changed = layout.changed;
    layout.changed = (model, action) => { changed(model, action); schedule(); };
    window.addEventListener("pagehide", save);
    return () => { clearTimeout(timer); save(); unsubscribe(); layout.changed = changed; window.removeEventListener("pagehide", save); };
  }, [layout, session]);
  return <WorkspaceContext.Provider value={value}><div className="vein-ide">
    <Header /><WorkspaceDock /><Status />
    {directoryInput}
  </div></WorkspaceContext.Provider>;
}
