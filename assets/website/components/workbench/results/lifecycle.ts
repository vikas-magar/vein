import type { QueryDocument } from "../workspace/contracts";

export type ResultState = "not-run" | "running" | "completed" | "failed" | "stopped" | "released";

/** Execution outcome and payload retention are independent; a failed rerun can retain older rows. */
export function resultState(document: QueryDocument | undefined): ResultState {
  if (!document) return "not-run";
  switch (document.execution) {
    case "running": case "failed": case "stopped": return document.execution;
  }
  if (document.releasedResult && !document.result) return "released";
  return document.result ? "completed" : "not-run";
}

export function resultCaption(document: QueryDocument | undefined) {
  const state = resultState(document);
  const labels: Record<ResultState, string> = {
    "not-run": "Not run", running: "Running", completed: "Completed", failed: "Failed",
    stopped: "Stopped", released: "Result released",
  };
  return labels[state] + (document?.result && ["running", "failed", "stopped"].includes(state)
    ? " · previous result retained" : "");
}
