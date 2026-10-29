import type { Table } from "apache-arrow";

export interface QueryExample { id: string; title: string; query: string }
export interface IoStatistics { bytesRead: number; readCalls: number; maxReadBytes: number }
export interface QueryResult {
  byteLength: number; table: Table; query: string;
  transport?: { packets: number; byteLength: number; maxPacketBytes: number; acknowledgementMs: number };
  source: SourceIdentity | null;
  engineMs: number; elapsedMs: number; ioStatistics?: IoStatistics;
}
export interface ResultOrigin { documentName: string; statementNumber: number | null }
export type ExecutionState = "idle" | "running" | "completed" | "failed" | "stopped";
export type ReleaseReason = "manual" | "memory-budget";
export interface ReleasedResult {
  query: string; source: SourceIdentity | null; origin: ResultOrigin | null;
  rows: number; reason: ReleaseReason;
}
export interface QueryDocument {
  id: string; name: string; text: string; original: string;
  result: QueryResult | null; resultOrigin: ResultOrigin | null;
  error: string; execution: ExecutionState; releasedResult: ReleasedResult | null;
  submitted: string; submittedStatementNumber: number | null;
}
export interface SourceIdentity { readonly id: string; readonly name: string }
export interface Dataset extends SourceIdentity { readonly files: number; readonly bytes: number; readonly tables: readonly string[] }
export interface OperationProgress {
  stage: "starting" | "registering" | "executing" | "decoding";
  completed?: number; total?: number;
}
export interface EngineSnapshot {
  phase: "idle" | "loading" | "ready" | "running" | "error";
  message: string; error: string; dataset: Dataset | null; result: QueryResult | null;
  progress?: OperationProgress | null;
}
export interface HistoryEntry { id: string; name: string; query: string; elapsedMs: number; rows: number }
