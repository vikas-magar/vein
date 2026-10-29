import { useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { Inspection } from "../query/inspection-types";

/** Render only visible schema rows; metadata comes directly from the bound table schema. */
export default function Columns({ table, columns }: { table: string; columns: Inspection["columns"] }) {
  const parent = useRef<HTMLDivElement>(null);
  const rows = useVirtualizer({ count: columns.length, getScrollElement: () => parent.current,
    estimateSize: () => 28, overscan: 4 });
  return <div ref={parent} className="ide-schema-columns" role="list" aria-label={`Columns of ${table}`}
    style={{ height: Math.min(240, columns.length * 28) }}>
    <div style={{ height: rows.getTotalSize(), position: "relative" }}>
      {rows.getVirtualItems().map(row => {
        const column = columns[row.index];
        return <div key={row.key} className="ide-schema-column" role="listitem"
          aria-posinset={row.index + 1} aria-setsize={columns.length}
          style={{ height: row.size, transform: `translateY(${row.start}px)` }}>
          <span title={column.name}>{column.name}</span><code title={column.data_type}>{column.data_type}</code>
        </div>;
      })}
    </div>
  </div>;
}
