import type { QueryDocument, QueryResult, ReleaseReason, ResultOrigin } from "./contracts.ts";

type Listener = () => void;
const DEFAULT_RESULT_BUDGET = 64 * 1024 * 1024;

/** Document subscriptions are independent: typing cannot invalidate another tab or result grid. */
export class Documents {
  private records = new Map<string, QueryDocument>();
  private listeners = new Map<string, Set<Listener>>();
  private indexListeners = new Set<Listener>();
  private changeListeners = new Set<Listener>();
  private revision = 0;
  private sequence = 0;
  readonly editorStates = new Map<string, unknown>();
  constructor(private resultBudget = DEFAULT_RESULT_BUDGET) {}
  get = (id: string) => this.records.get(id);
  list = () => [...this.records.values()];
  version = () => this.revision;
  subscribeIndex = (listener: Listener) => {
    this.indexListeners.add(listener);
    return () => { this.indexListeners.delete(listener); };
  };
  subscribeChanges = (listener: Listener) => {
    this.changeListeners.add(listener); return () => { this.changeListeners.delete(listener); };
  };
  subscribe = (id: string, listener: Listener) => {
    const listeners = this.listeners.get(id) ?? new Set<Listener>();
    this.listeners.set(id, listeners); listeners.add(listener);
    return () => { listeners.delete(listener); };
  };
  open(name: string, text: string, id?: string) {
    if (!id) { do { id = `query-${++this.sequence}`; } while (this.records.has(id)); }
    if (!this.records.has(id)) {
      this.records.set(id, { id, name, text, original: text, result: null, resultOrigin: null,
        error: "", execution: "idle", releasedResult: null, submitted: "", submittedStatementNumber: null });
      this.changed();
    }
    return id;
  }
  edit(id: string, text: string) {
    const current = this.get(id);
    if (!current || current.text === text) return;
    const wasDirty = current.text !== current.original;
    this.patch(id, { text });
    if (wasDirty !== (text !== current.original)) this.changed();
  }
  rename(id: string, name: string) {
    if (!name.trim() || !this.get(id)) return;
    this.patch(id, { name: name.trim() }); this.changed();
  }
  patch(id: string, update: Partial<QueryDocument>) {
    const current = this.get(id);
    if (!current) return;
    this.records.set(id, { ...current, ...update });
    this.listeners.get(id)?.forEach(listener => listener());
    this.changeListeners.forEach(listener => listener());
  }
  complete(id: string, result: QueryResult, resultOrigin: ResultOrigin | null = null) {
    this.patch(id, { result, resultOrigin, releasedResult: null, execution: "completed", error: "" });
    // Evict old results by reference. Never slice/copy Arrow payloads to meet a budget.
    const retained = this.list().filter(document => document.result);
    let bytes = retained.reduce((sum, document) => sum + document.result!.byteLength, 0);
    for (const document of retained) {
      if (bytes <= this.resultBudget) break;
      if (document.id === id) continue;
      bytes -= document.result!.byteLength;
      this.release(document.id, "memory-budget");
    }
  }
  release(id: string, reason: ReleaseReason) {
    const document = this.get(id);
    if (!document?.result) return;
    const { query, source, table } = document.result;
    this.patch(id, { result: null, resultOrigin: null, releasedResult: {
      query, source, rows: table.numRows, origin: document.resultOrigin, reason,
    } });
  }
  close(id: string) {
    this.records.delete(id); this.editorStates.delete(id);
    this.listeners.get(id)?.forEach(listener => listener());
    this.listeners.delete(id); this.changed();
  }
  private changed() {
    this.revision++; this.indexListeners.forEach(listener => listener());
    this.changeListeners.forEach(listener => listener());
  }
}
