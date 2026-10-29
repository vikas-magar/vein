import type { SourceIdentity } from "../workspace/contracts";

export type ResultSourceState = "unknown" | "disconnected" | "current" | "previous";

/** Directory names cannot prove source identity; compare the retained selection IDs only. */
export function resultSourceState(source: SourceIdentity | null | undefined, current: SourceIdentity | null): ResultSourceState {
  if (!source) return "unknown";
  if (!current) return "disconnected";
  return source.id === current.id ? "current" : "previous";
}

export function resultSourceLabel(state: ResultSourceState) {
  switch (state) {
    case "unknown": return "Source unavailable";
    case "disconnected": return "Source not connected";
    case "current": return "Current data selection";
    case "previous": return "Previous data selection";
  }
}
