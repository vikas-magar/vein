import { useState } from "react";
import { getWorkspaceSession } from "../workspace/session";
import { WorkspaceLayout } from "../layout/controller";
import { WorkspaceContext } from "../workspace/hooks";
import { useWorkspaceActions } from "../workspace/actions";
import QueryPanel from "../query/panel";
import ResultsPanel from "../results/panel";
import "../styles/ide.css";

export default function LiveExample({ id, query }: { id: string; query: string }) {
  const [session] = useState(getWorkspaceSession);
  const [layout] = useState(() => {
    const value = new WorkspaceLayout(session.documents);
    value.open("Documentation example", query, `docs:${id}`);
    return value;
  });
  const actions = useWorkspaceActions(session, layout);
  return <WorkspaceContext.Provider value={actions.value}>
    <div className="document-live"><QueryPanel documentId={`docs:${id}`} /><ResultsPanel /></div>
    {actions.directoryInput}
  </WorkspaceContext.Provider>;
}
