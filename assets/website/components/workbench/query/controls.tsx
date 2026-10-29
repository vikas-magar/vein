import { Button, Tooltip, TooltipTrigger } from "react-aria-components";
import { memo } from "react";
import { useActiveDocument, useDocumentValue, useEngineSnapshot, useWorkspace } from "../workspace/hooks";
import ExecutionProgress from "../workspace/progress";
import Icon from "../icon";

export default memo(function QueryControls({ documentId, query, statementNumber }: {
  documentId: string; query: string; statementNumber: number | null;
}) {
  const running = useDocumentValue(documentId, document => document?.execution === "running");
  const ownsRun = useDocumentValue(documentId, document => document?.execution === "running"
    && document.submitted === query && document.submittedStatementNumber === statementNumber);
  const engine = useEngineSnapshot();
  const activeId = useActiveDocument();
  const { session, run } = useWorkspace();
  const busy = engine.phase === "running" || engine.phase === "loading";
  const loading = (statementNumber === null || statementNumber === 1) && engine.phase === "loading"
    && activeId === documentId && !running;
  return <div className="ide-query-actions" role="group" aria-label="Query execution">
    <TooltipTrigger><Button className="ide-query-action ide-query-play" aria-label="Run"
      isDisabled={busy || !query.trim()} onPress={() => run(documentId, query, statementNumber ?? undefined)}>
      <Icon name="play" size={16} />
    </Button><Tooltip className="ide-query-tooltip">Run query (Ctrl / ⌘ Enter)</Tooltip></TooltipTrigger>
    <TooltipTrigger><Button className="ide-query-action" isDisabled={!busy || (!ownsRun && !loading)}
      onPress={session.stop} aria-label="Stop execution">
      <Icon name="stop" size={16} />
    </Button><Tooltip className="ide-query-tooltip">Stop execution</Tooltip></TooltipTrigger>
    {(ownsRun || loading) && <ExecutionProgress />}
  </div>;
});
