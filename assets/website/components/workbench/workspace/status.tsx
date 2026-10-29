import { useActiveDocument, useDocument, useEngineSnapshot } from "./hooks";
import { formatBytes } from "../results/format.mjs";
import { resultCaption, resultState } from "../results/lifecycle";
import ExportResult from "../results/export";

export default function StatusBar() {
  const engine = useEngineSnapshot();
  const id = useActiveDocument();
  const document = useDocument(id);
  const result = document?.result;
  const dataset = engine.dataset;
  const state = resultState(document);
  const active = ["loading", "running", "error"].includes(engine.phase);
  const message = active || state === "not-run" ? engine.message : resultCaption(document);
  return <footer className="ide-status" role="group" aria-label="Result summary">
    {dataset && <span className="ide-status-source"><strong title={dataset.name}>{dataset.name}</strong>
      <span>{dataset.files} {dataset.files === 1 ? "file" : "files"} · {formatBytes(dataset.bytes)}</span></span>}
    <div className="ide-status-details">
    <span role="status" data-phase={engine.phase} data-state={state} className="ide-status-message" title={message}>
      <span className={`ide-dot${engine.phase === "error" || state === "failed" ? " is-error" : ""}`} />
      <span>{message}</span>
    </span>
    {result && <><span>{result.table.numRows.toLocaleString()} rows</span>
      <span>{result.elapsedMs.toFixed(1)} ms</span></>}
    {result && <><span>{formatBytes(result.byteLength)} result</span>
      {result.ioStatistics && <span>{formatBytes(result.ioStatistics.bytesRead)} read · {result.ioStatistics.readCalls} ranges</span>}
      <ExportResult result={result} name={document?.name ?? "query"} /></>}
    <span className="ide-status-host">WASM · local</span>
    </div>
  </footer>;
}
