import { useActiveDocument, useDocument, useEngineSnapshot } from "../workspace/hooks";
import { formatBytes } from "./format.mjs";
import { resultSourceLabel, resultSourceState } from "./source";
import { resultCaption } from "./lifecycle";

export default function DetailsPanel({ messages = false }: { messages?: boolean }) {
  const id = useActiveDocument();
  const document = useDocument(id);
  const engine = useEngineSnapshot();
  const result = document?.result;
  if (messages) return <section className="ide-details" aria-label="Execution messages">
    <h2>Execution output</h2>
    {document?.error && <pre className={document.execution === "stopped" ? "ide-execution-note" : "ide-error"}
      role={document.execution === "stopped" ? "note" : "alert"}>{document.error}</pre>}
    <p>{document?.execution === "running" ? engine.message : resultCaption(document)}</p>
    {document?.submitted && <pre>{document.submitted}</pre>}
  </section>;
  return <section className="ide-details" aria-label="Execution details"><h2>Execution details</h2>
    <p>Measured values from this browser session.</p>
    <dl>
      <dt>Connected source</dt><dd>{engine.dataset?.name ?? "Not connected"}</dd>
      <dt>Query state</dt><dd>{resultCaption(document)}</dd>
      {result && <><dt>Result source</dt><dd>{result.source?.name ?? "Unavailable"} · {resultSourceLabel(resultSourceState(result.source, engine.dataset))}</dd></>}
      <dt>Execution host</dt><dd>Vein WASM · dedicated Worker</dd>
      <dt>Result rows</dt><dd>{result?.table.numRows.toLocaleString() ?? "—"}</dd>
      <dt>Engine elapsed</dt><dd>{result ? `${result.engineMs.toFixed(1)} ms` : "—"}</dd>
      <dt>Total elapsed</dt><dd>{result ? `${result.elapsedMs.toFixed(1)} ms` : "—"}</dd>
      <dt>Result bytes transferred</dt><dd>{result ? formatBytes(result.byteLength) : "—"}</dd>
      {result?.transport && <><dt>IPC packets</dt><dd>{result.transport.packets}</dd>
        <dt>Largest IPC packet</dt><dd>{formatBytes(result.transport.maxPacketBytes)}</dd>
        <dt>Consumer wait</dt><dd>{result.transport.acknowledgementMs.toFixed(1)} ms</dd></>}
      <dt>Browser bytes read</dt><dd>{result?.ioStatistics ? formatBytes(result.ioStatistics.bytesRead) : "—"}</dd>
      <dt>Range reads</dt><dd>{result?.ioStatistics?.readCalls ?? "—"}</dd>
      <dt>Largest range</dt><dd>{result?.ioStatistics ? formatBytes(result.ioStatistics.maxReadBytes) : "—"}</dd>
    </dl>
    <p className="ide-footnote">Engine elapsed includes preparation and result encoding, excluding acknowledgement waits. Total elapsed includes transfer and decoding. Browser reads exclude internal OS caching.</p>
  </section>;
}
