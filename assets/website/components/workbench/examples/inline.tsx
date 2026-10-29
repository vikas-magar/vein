"use client";

import { useState } from "react";
import dynamic from "next/dynamic";
import type { ReactNode } from "react";

const LiveExample = dynamic(() => import("./live"), { ssr: false,
  loading: () => <p className="document-lead">Starting local query editor…</p> });

/** Documentation remains static until the visitor explicitly opens an editor. */
export default function InlineExample({ id, query, children }: { id: string; query: string; children?: ReactNode }) {
  const [live, setLive] = useState(false);
  return <div className="document-example">
    {!live ? <><div className="document-example-toolbar"><span>VEIN PIPE</span>
      <button className="button button-secondary" onClick={() => setLive(true)}>Edit & run</button></div>
      {children ?? <pre><code>{query}</code></pre>}</> : <LiveExample id={id} query={query} />}
  </div>;
}
