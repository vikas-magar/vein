"use client";

import dynamic from "next/dynamic";

const Workspace = dynamic(() => import("./workspace"), {
  ssr: false,
  loading: () => <div className="workspace-starting" role="status">Starting Vein workspace…</div>,
});

export default function QueryWorkbench() { return <Workspace />; }
