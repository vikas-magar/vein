import type { IJsonModel } from "flexlayout-react";
import type { Documents } from "./documents";

const KEY = "vein.workspace.v1";
const MAX_CHARACTERS = 256 * 1024;
interface Storage { getItem(key: string): string | null; setItem(key: string, value: string): void }

/** Persists text and layout only. File handles, Arrow buffers and query results stay in memory. */
export function restoreWorkspace(storage: Storage, documents: Documents): IJsonModel | undefined {
  try {
    const raw = storage.getItem(KEY);
    if (!raw || raw.length > MAX_CHARACTERS) return;
    const saved = JSON.parse(raw);
    if (saved.version !== 1 || !Array.isArray(saved.documents) || saved.documents.length > 1000) return;
    for (const item of saved.documents) {
      if (![item.id, item.name, item.text, item.original].every(value => typeof value === "string")
        || item.text.length > 100_000) continue;
      documents.open(item.name, item.text, item.id);
      documents.patch(item.id, { original: item.original });
    }
    return saved.layout;
  } catch { return; }
}

export function saveWorkspace(storage: Storage, documents: Documents, layout: IJsonModel) {
  try {
    const drafts = documents.list().map(({ id, name, text, original }) => ({ id, name, text, original }));
    const serialized = JSON.stringify({ version: 1, documents: drafts, layout });
    if (serialized.length <= MAX_CHARACTERS) storage.setItem(KEY, serialized);
  } catch { /* Private browsing or quota failures must never interrupt a query. */ }
}
