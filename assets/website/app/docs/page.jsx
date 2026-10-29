import Link from "next/link";
import InlineExample from "../../components/workbench/examples/inline";
import HighlightedCode from "../../components/workbench/query/highlight";

const shipmentsQuery = "FROM lineitem\n|> WHERE l_shipmode = 'MAIL'\n|> AGGREGATE count(*) AS shipments";

export const metadata = { title: "Documentation" };

export default function DocsPage() {
  return <div className="page-width document-layout">
    <aside className="docs-nav"><p className="eyebrow">GET STARTED</p>
      <a href="#overview">Introduction</a><a href="#quick-start">CLI quick start</a>
      <a href="#data-layout">Data directories</a><a href="#pipe">Write a query</a>
      <a href="#workspace">Browser workspace</a><a href="#limits">Current limits</a>
    </aside>
    <article className="document">
      <p className="eyebrow">VEIN DOCUMENTATION</p>
      <h1>Explore your data.<br />Keep it local.</h1>
      <p className="document-lead" id="overview">Vein is a single-node analytical engine with composable Pipe queries. Use the native CLI or run the same engine as WebAssembly inside your browser.</p>
      <section id="quick-start"><h2>Run your first query</h2>
        <p>Set your table-directory root and pass the query as one quoted argument.</p>
        <pre><code>{`VEIN_DATA_DIR=/path/to/tpch cargo run -p cli -- "
FROM lineitem
|> AGGREGATE count(*) AS row_count
"`}</code></pre>
      </section>
      <section id="data-layout"><h2>Choose a data directory</h2>
        <p>Each table has its own folder. Vein discovers Parquet files and reads their metadata when the table is queried.</p>
        <pre><code>{`tpch/
├── lineitem/
│   ├── lineitem.1.parquet
│   └── lineitem.2.parquet
├── orders/
│   └── orders.parquet
└── customer/
    └── customer.parquet`}</code></pre>
        <p>In the browser, choose the <code>tpch</code> directory. Registration keeps file references; it does not upload or preload the complete dataset. Queries read requested byte ranges locally.</p>
      </section>
      <section id="pipe"><h2>Compose a query, one stage at a time</h2>
        <p>Start with <code>FROM</code>, then add filters, joins, projections and aggregates. Open the editor below to run this example against your own TPC-H directory.</p>
        <InlineExample id="shipments" query={shipmentsQuery}><HighlightedCode query={shipmentsQuery} /></InlineExample>
        <p>The editor runs the real Vein engine. It requires matching Parquet tables; no simulated results are displayed.</p>
      </section>
      <section id="workspace"><h2>A workspace that stays with you</h2>
        <ul><li>Open Q1–Q22 from the Examples panel as independent query tabs.</li>
          <li>Execute with <code>Ctrl+Enter</code> or <code>⌘+Enter</code>.</li>
          <li>Resize and dock panels, inspect results, and export Arrow IPC.</li>
          <li>Query drafts and layout are saved locally. After a page reload, reselect your data directory.</li>
          <li>Stopping execution terminates the Worker. Its selected file references remain available for reconnecting on the next run.</li>
        </ul>
        <Link className="button button-secondary" href="/">Open workspace ↗</Link>
      </section>
      <section id="limits"><h2>Alpha limits</h2><p>Browser mode is read-only and does not provide disk spill. Working query state and result buffers must fit available browser memory. Browser Parquet codec support and query coverage remain under development.</p>
        <p>Results use a virtualized grid and exact decimal formatting. Rendering reuses Arrow columns; file-to-WASM reads, result encoding and text display still require allocations. Vein does not currently expose a stable JDBC or Flight SQL endpoint.</p>
      </section>
    </article>
  </div>;
}
