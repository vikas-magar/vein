import type { Inspection } from "../query/inspection-types";
import { quoteIdentifier } from "../query/inspection-context";

export interface SchemaProvider {
  inspect: (query: string, prefix?: string) => Promise<Inspection | null>;
  getSnapshot: () => { phase: string; dataset: { id: string } | null };
}
export type TableSchema = { columns: Inspection["columns"]; error: string | null; stale: boolean };

/** Always quote catalog table names, including names that coincide with language keywords. */
export function tableQuery(table: string) { return `FROM ${quoteIdentifier(table, true)}`; }

/** Reuses the bounded inspection channel; stale metadata is discarded before reaching the view. */
export async function inspectTable(provider: SchemaProvider, table: string, sourceId: string): Promise<TableSchema> {
  const current = () => provider.getSnapshot().dataset?.id === sourceId;
  if (!current()) return { columns: [], error: null, stale: true };
  if (provider.getSnapshot().phase !== "ready") {
    return { columns: [], error: "Wait for the current operation before loading the schema.", stale: false };
  }
  const query = tableQuery(table);
  const inspection = await provider.inspect(query, query);
  if (!current()) return { columns: [], error: null, stale: true };
  if (!inspection) return { columns: [], error: "Schema temporarily unavailable. Try again when the engine is ready.", stale: false };
  return { columns: inspection.columns, error: inspection.diagnostics.map(error => error.message).join("\n") || null, stale: false };
}
