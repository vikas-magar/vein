import { memo, useEffect, useId, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { KeyboardEvent } from "react";
import type { Table } from "apache-arrow";
import { ArrowGridModel } from "./arrow-model";

const ROW_HEIGHT = 30;
const COLUMN_WIDTH = 190;
const INDEX_WIDTH = 48;

export default memo(function ResultGrid({ table }: { table: Table }) {
  const parent = useRef<HTMLDivElement>(null);
  const id = useId();
  const model = useMemo(() => new ArrowGridModel(table), [table]);
  const [selected, select] = useState({ row: 0, column: 0 });
  const resize = useRef<{ index: number; x: number; width: number } | null>(null);
  const rows = useVirtualizer({ count: model.rows, getScrollElement: () => parent.current,
    estimateSize: () => ROW_HEIGHT, overscan: 5, paddingStart: ROW_HEIGHT });
  const columns = useVirtualizer({ horizontal: true, count: model.columnCount,
    getScrollElement: () => parent.current, estimateSize: () => COLUMN_WIDTH, overscan: 2 });
  const width = columns.getTotalSize() + INDEX_WIDTH;
  useEffect(() => {
    select({ row: 0, column: 0 });
    rows.scrollToOffset(0); columns.scrollToOffset(0);
  }, [table, rows, columns]);
  function navigate(event: KeyboardEvent) {
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "c") {
      if (model.rows) void navigator.clipboard?.writeText(model.text(selected.row, selected.column));
      return;
    }
    const deltas: Record<string, [number, number]> = {
      ArrowDown: [1, 0], ArrowUp: [-1, 0], ArrowRight: [0, 1], ArrowLeft: [0, -1],
      PageDown: [15, 0], PageUp: [-15, 0],
    };
    if (!deltas[event.key]) return;
    event.preventDefault();
    const [dr, dc] = deltas[event.key];
    const next = { row: Math.max(0, Math.min(model.rows - 1, selected.row + dr)),
      column: Math.max(0, Math.min(model.columnCount - 1, selected.column + dc)) };
    select(next); rows.scrollToIndex(next.row); columns.scrollToIndex(next.column);
  }
  if (!model.rows) return <div className="ide-empty"><strong>Query completed</strong><p>No rows matched this query.</p></div>;
  return <div ref={parent} className="ide-grid" role="grid" tabIndex={0} aria-label="Query result grid"
    aria-rowcount={model.rows + 1} aria-colcount={model.columnCount + 1}
    aria-activedescendant={`${id}-${selected.row}-${selected.column}`} onKeyDown={navigate}>
    <div style={{ width, height: rows.getTotalSize(), position: "relative", minWidth: "100%" }}>
      <div className="ide-grid-header" role="row" aria-rowindex={1} style={{ width }}>
        <div className="ide-row-number" role="columnheader" style={{ width: INDEX_WIDTH }}>#</div>
        {columns.getVirtualItems().map(column => <div role="columnheader" className="ide-grid-cell" key={column.key}
          aria-colindex={column.index + 2} style={{ left: INDEX_WIDTH + column.start, width: column.size }}
          title={`${model.fields[column.index].name}: ${model.fields[column.index].type}`}>
          {model.fields[column.index].name}<span>{String(model.fields[column.index].type)}</span>
          <div role="separator" className="ide-column-resize" tabIndex={0} aria-orientation="vertical"
            aria-label={`Resize ${model.fields[column.index].name} column`} aria-valuenow={column.size}
            aria-valuemin={80} aria-valuemax={600}
            onKeyDown={event => {
              if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
              event.preventDefault(); event.stopPropagation();
              columns.resizeItem(column.index, Math.max(80, Math.min(600, column.size + (event.key === "ArrowRight" ? 16 : -16))));
            }}
            onPointerDown={event => {
              event.preventDefault(); event.currentTarget.setPointerCapture(event.pointerId);
              resize.current = { index: column.index, x: event.clientX, width: column.size };
            }}
            onPointerMove={event => {
              if (resize.current?.index === column.index) columns.resizeItem(column.index,
                Math.max(80, Math.min(600, resize.current.width + event.clientX - resize.current.x)));
            }}
            onPointerUp={() => { resize.current = null; }} onPointerCancel={() => { resize.current = null; }} />
        </div>)}
      </div>
      {rows.getVirtualItems().map(row => <div role="row" key={row.key} aria-rowindex={row.index + 2}
        className="ide-grid-row" style={{ transform: `translateY(${row.start}px)`, width, height: row.size }}>
        <div className="ide-row-number" role="rowheader" style={{ width: INDEX_WIDTH }}>{row.index + 1}</div>
        {columns.getVirtualItems().map(column => <div role="gridcell" key={column.key}
          id={`${id}-${row.index}-${column.index}`} aria-colindex={column.index + 2}
          aria-selected={row.index === selected.row && column.index === selected.column}
          className={`ide-grid-cell${model.isNull(row.index, column.index) ? " is-null" : ""}`}
          style={{ left: INDEX_WIDTH + column.start, width: column.size }}
          title={model.text(row.index, column.index)}
          onClick={() => { select({ row: row.index, column: column.index }); parent.current?.focus(); }}>
          {model.text(row.index, column.index)}
        </div>)}
      </div>)}
    </div>
  </div>;
});
