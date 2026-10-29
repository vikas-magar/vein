import { useEffect, useState } from "react";
import { assetUrl } from "./assets.mjs";

export default function useQueryLibrary() {
  const [state, setState] = useState({ queries: [], loading: true, error: "" });
  useEffect(() => {
    const controller = new AbortController();
    fetch(assetUrl("queries/tpch-pipe.json"), { signal: controller.signal })
      .then(response => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then(queries => {
        if (!Array.isArray(queries) || !queries.length || !queries.every(item =>
          typeof item.id === "string" && typeof item.title === "string" && typeof item.query === "string")) {
          throw new Error("The query library is invalid");
        }
        if (!controller.signal.aborted) setState({ queries, loading: false, error: "" });
      })
      .catch(cause => {
        if (!controller.signal.aborted) setState({ queries: [], loading: false, error: cause.message });
      });
    return () => controller.abort();
  }, []);
  return state;
}
