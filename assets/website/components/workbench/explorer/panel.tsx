import { useState } from "react";
import { Button, SearchField, Input, Label } from "react-aria-components";
import useQueryLibrary from "../use-query-library";
import { useEngineSnapshot, useWorkspace } from "../workspace/hooks";
import Icon from "../icon";
import type { QueryExample } from "../workspace/contracts";
import TableEntry from "./table";

export default function ExplorerPanel({ mode }: { mode: "explorer" | "examples" | "history" }) {
  const [filter, setFilter] = useState("");
  const library = useQueryLibrary();
  const engine = useEngineSnapshot();
  const dataset = engine.dataset;
  const { session, layout, openDirectory } = useWorkspace();
  const search = filter.toLowerCase().trim();
  const title = mode === "explorer" ? "Tables" : mode === "examples" ? "Examples" : "History";
  const examples: QueryExample[] = library.queries;
  return <section className="ide-explorer" aria-label={`${title} explorer`}>
    <SearchField className="ide-search" value={filter} onChange={setFilter}>
      <Label className="ide-sr-only">Search {title.toLowerCase()}</Label>
      <Icon name="search" size={14} /><Input placeholder={`Find ${title.toLowerCase()}…`} />
    </SearchField>
    {mode === "explorer" && <div className="ide-source-heading">
      <span>DATA DIRECTORY</span><Button className="ide-tool" onPress={openDirectory} aria-label="Open data directory"><Icon name="folder" size={14} /></Button>
    </div>}
    <div className="ide-explorer-list">
      {mode === "explorer" && (dataset ? <>
        <div className="ide-tree-root"><Icon name="database" />{dataset.name}<span className="ide-dot" /></div>
        {dataset.tables.filter(table => table.toLowerCase().includes(search)).map(table =>
          <TableEntry key={`${dataset.id}:${table}`} table={table} sourceId={dataset.id} />)}
      </> : <div className="ide-explorer-empty"><Icon name="folder" size={26} /><strong>Bring your data</strong>
        <p>Select the directory containing your Parquet table folders. Files stay on your device.</p>
        <Button className="ide-primary" onPress={openDirectory}>Open data directory</Button></div>)}
      {mode === "examples" && <>
        <div className="ide-source-heading">TPC-H · Q1–Q22</div>
        {examples.filter(item => `${item.id} ${item.title}`.toLowerCase().includes(search)).map(item =>
          <Button key={item.id} className="ide-example" onPress={() => layout.open(`${item.id}.vein`, item.query, `example:${item.id}`)}>
            <span>{item.id}</span><div>{item.title}<small>Vein Pipe</small></div><Icon name="arrow" size={12} />
          </Button>)}
        {library.loading && <p className="ide-explorer-note">Loading examples…</p>}
        {library.error && <p className="ide-error">{library.error}</p>}
      </>}
      {mode === "history" && <>
        <div className="ide-source-heading">RECENT EXECUTIONS</div>
        {session.history.filter(item => `${item.name} ${item.query}`.toLowerCase().includes(search)).map(item =>
          <Button key={item.id} className="ide-history" onPress={() => layout.open(item.name, item.query)}>
            <Icon name="file" size={14} /><div>{item.name}<small>{item.rows.toLocaleString()} rows · {item.elapsedMs.toFixed(1)} ms</small></div>
          </Button>)}
        {!session.history.length && <p className="ide-explorer-note">Successful query executions appear here.</p>}
      </>}
    </div>
  </section>;
}
