import { useEffect, useId, useState } from "react";
import { Button } from "react-aria-components";
import { useEngineSnapshot, useWorkspace } from "../workspace/hooks";
import type { Inspection } from "../query/inspection-types";
import Columns from "./columns";
import { inspectTable, tableQuery } from "./schema";
import Icon from "../icon";

export default function TableEntry({ table, sourceId }: { table: string; sourceId: string }) {
  const { session, layout } = useWorkspace();
  const engine = useEngineSnapshot();
  const id = useId();
  const [expanded, setExpanded] = useState(false);
  const [columns, setColumns] = useState<Inspection["columns"] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [retry, setRetry] = useState(0);
  useEffect(() => {
    if (!expanded || columns !== null) return;
    let cancelled = false;
    setLoading(true); setError(null);
    inspectTable(session, table, sourceId).then(schema => {
      if (cancelled || schema.stale) return;
      setLoading(false);
      if (schema.error) setError(schema.error);
      else setColumns(schema.columns);
    }).catch(() => {
      if (!cancelled && session.snapshot.dataset?.id === sourceId) {
        setLoading(false); setError("Schema could not be loaded. Try again.");
      }
    });
    return () => { cancelled = true; };
  }, [expanded, columns, retry, session, table, sourceId]);
  const busy = engine.phase !== "ready";
  return <div className="ide-table-entry">
    <div className="ide-table-row">
      <Button className="ide-schema-toggle" aria-expanded={expanded} aria-controls={id}
        aria-label={`${expanded ? "Collapse" : "Expand"} table ${table}`}
        isDisabled={busy && !expanded} onPress={() => setExpanded(value => !value)}>
        <Icon name="arrow" size={12} />
      </Button>
      <Button className="ide-tree-item" aria-label={`Open query for ${table}`} onPress={() => layout.open(
        `${table}.vein`, `${tableQuery(table)}\n|> AGGREGATE count(*) AS row_count`, `table:${sourceId}:${table}`,
      )}><Icon name="table" size={14} /><span>{table}</span><small>TABLE</small></Button>
    </div>
    {expanded && <div id={id} className="ide-table-schema" aria-busy={loading}>
      {loading && <p className="ide-schema-note">Loading schema…</p>}
      {error && <div className="ide-schema-note"><p>{error}</p>
        <Button className="ide-tool" isDisabled={busy || loading} onPress={() => setRetry(value => value + 1)}>Retry schema</Button>
      </div>}
      {columns !== null && (columns.length ? <Columns table={table} columns={columns} />
        : <p className="ide-schema-note">This table has no columns.</p>)}
    </div>}
  </div>;
}
