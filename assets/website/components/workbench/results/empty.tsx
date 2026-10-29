import { Button } from "react-aria-components";
import type { QueryDocument } from "../workspace/contracts";
import { useEngineSnapshot, useWorkspace } from "../workspace/hooks";
import { resultState } from "./lifecycle";
import Icon from "../icon";

export default function EmptyResult({ document }: { document: QueryDocument | undefined }) {
  const engine = useEngineSnapshot();
  const { run, openDirectory } = useWorkspace();
  const state = resultState(document);
  const released = document?.releasedResult;
  const retryQuery = state === "released" ? released?.query : document?.submitted;
  const heading = state === "released" ? released?.reason === "memory-budget"
    ? "Result released to save memory" : "Result released"
    : state === "running" ? "Executing query" : state === "failed" ? "Query failed"
    : state === "stopped" ? "Execution stopped" : "Your next result starts here";
  const description = state === "released" ? "The result data was released. Your query is still available."
    : state === "running" ? engine.message : state === "failed" ? "The latest execution did not produce a new result."
    : state === "stopped" ? "Execution ended without producing a new result."
    : "Open a data directory, choose an example, and run it locally.";
  function retry() {
    if (!document) return;
    const query = state === "released" ? released?.query : document.submitted;
    const number = state === "released" ? released?.origin?.statementNumber : document.submittedStatementNumber;
    if (query) run(document.id, query, number ?? undefined);
  }
  const busy = engine.phase === "loading" || engine.phase === "running";
  return <div className="ide-empty">
    <Icon name="database" size={30} /><strong>{heading}</strong><p>{description}</p>
    {["released", "failed", "stopped"].includes(state) && retryQuery && <>
      <Button className="ide-primary" onPress={retry} isDisabled={busy}>
        {state === "released" ? "Rerun released query" : "Retry query"}
      </Button>
      <p>{engine.dataset ? "Runs the statement on the current data selection." : "Choose a data directory to run the statement."}</p>
    </>}
    {state === "not-run" && !engine.dataset && <Button className="ide-primary" onPress={openDirectory}>Open data directory</Button>}
  </div>;
}
