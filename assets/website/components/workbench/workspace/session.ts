import { EngineClient } from "../engine/client.mjs";
import { assetUrl } from "../assets.mjs";
import { Documents } from "./documents";
import type { EngineSnapshot, HistoryEntry, ResultOrigin } from "./contracts";
import { captureResultOrigin } from "../results/origin";

/** Page-lifetime owner. React panels may mount/unmount without destroying this session. */
export class WorkspaceSession {
  readonly documents = new Documents();
  private listeners = new Set<() => void>();
  private client: EngineClient;
  private files: File[] = [];
  private sourceSequence = 0;
  private sourceId = "";
  private pending: { id: string; query: string; origin: ResultOrigin } | null = null;
  private awaitingLoad = false;
  history: HistoryEntry[] = [];
  snapshot: EngineSnapshot = { phase: "idle", message: "Open a data directory to begin",
    error: "", dataset: null, result: null };
  constructor() {
    this.client = new EngineClient({
      workerFactory: () => new Worker(assetUrl("worker.js"), { type: "module" }),
      wasmModuleUrl: assetUrl("wasm/vein_wasm.js"),
      decodeResult: async (buffer: ArrayBuffer) => {
        const { tableFromIPC } = await import("apache-arrow");
        return tableFromIPC(new Uint8Array(buffer));
      },
      onChange: (state: EngineSnapshot) => this.receive(state),
    });
  }
  getSnapshot = () => this.snapshot;
  inspect = (query: string, prefix = query) => this.client.inspection.inspect(query, prefix);
  subscribe = (listener: () => void) => {
    this.listeners.add(listener); return () => { this.listeners.delete(listener); };
  };
  load = (files: File[]) => {
    if (this.pending) this.documents.patch(this.pending.id, {
      execution: "stopped", error: "Execution stopped because the data directory changed.",
    });
    this.files = files; this.pending = null; this.awaitingLoad = false;
    this.sourceId = `selection:${++this.sourceSequence}`;
    this.client.load(files, this.sourceId);
  };
  run = (id: string, queryOverride?: string, statementNumber?: number) => {
    const document = this.documents.get(id);
    const query = queryOverride ?? document?.text;
    if (!document || !query?.trim() || ["running", "loading"].includes(this.snapshot.phase)) return false;
    if (!this.files.length && !this.client.worker) return false;
    this.pending = { id, query, origin: captureResultOrigin(document, query, statementNumber) };
    this.documents.patch(id, { execution: "running", error: "", submitted: query,
      submittedStatementNumber: this.pending.origin.statementNumber });
    if (this.client.state.phase === "ready" && this.client.worker) return this.client.run(query);
    if (this.files.length) { this.awaitingLoad = true; return this.client.load(this.files, this.sourceId); }
    this.pending = null; this.documents.patch(id, { execution: "idle" }); return false;
  };
  stop = () => {
    if (this.pending) this.documents.patch(this.pending.id, { execution: "stopped", error: "Execution stopped." });
    this.pending = null; this.awaitingLoad = false; this.client.cancel();
  };
  releaseResult = (id: string) => this.documents.release(id, "manual");
  private receive(state: EngineSnapshot) {
    const pending = this.pending;
    if (pending && this.awaitingLoad && state.phase === "ready" && !state.error) {
      this.awaitingLoad = false; this.client.run(pending.query); return;
    }
    if (pending && state.result) {
      this.pending = null;
      this.documents.complete(pending.id, state.result, pending.origin);
      this.history = [{ id: `${Date.now()}`, name: this.documents.get(pending.id)?.name ?? "Closed query",
        query: pending.query, elapsedMs: state.result.elapsedMs, rows: state.result.table.numRows }, ...this.history].slice(0, 30);
      // Ownership moves to the document store; the transport must not retain a second result owner.
      this.client.clearResult(); return;
    }
    if (pending && state.error) {
      this.pending = null;
      this.documents.patch(pending.id, { execution: "failed", error: state.error });
    }
    this.snapshot = state;
    this.listeners.forEach(listener => listener());
  }
}

let session: WorkspaceSession | undefined;
export function getWorkspaceSession() { return session ??= new WorkspaceSession(); }
