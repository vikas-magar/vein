// Executes the real packaged WASM engine with genuine Parquet and a Node-only
// FileReaderSync adapter. Browser picker compatibility needs a browser check.
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { tableFromIPC } from "apache-arrow";
import { createEngineRuntime } from "../public/worker/engine-runtime.mjs";
import { formatCell } from "../components/workbench/results/format.mjs";
import { ResultCollector } from "../components/workbench/engine/result-collector.mjs";
import { assembleArrowTables } from "../components/workbench/results/assemble.mjs";

const origin = process.env.VEIN_WEBSITE_URL ?? "http://127.0.0.1:3000";

async function remoteModule(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${url}`);
  let source = await response.text();
  for (const [specifier, , relative] of [...source.matchAll(/from\s+(['"])([^'"]+)\1/g)]) {
    const child = await remoteModule(new URL(relative, url));
    source = source.replace(specifier, `from ${JSON.stringify(child)}`);
  }
  return `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
}

async function validate(directory) {
  const path = `${directory}/smoke.parquet`;
  await promisify(execFile)("duckdb", ["-c", `COPY (
    SELECT i::BIGINT AS id, (i * 10)::BIGINT AS amount,
      repeat('unprojected payload', 16384) AS unused
    FROM range(1, 4) t(i)
  ) TO '${path}' (FORMAT PARQUET)`]);
  const bytes = await readFile(path);
  const file = new File([bytes], "smoke.parquet");
  file.slice = (start, end) => bytes.subarray(start, end);
  const unrelated = new File(["must never be read"], "unrelated.parquet");
  unrelated.slice = () => { throw new Error("An unqueried table was read"); };
  globalThis.FileReaderSync = class {
    readAsArrayBuffer(view) {
      return view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength);
    }
  };
  const module = await import(await remoteModule(new URL("/wasm/vein_wasm.js", origin)));
  const response = await fetch(new URL("/wasm/vein_wasm_bg.wasm", origin));
  if (!response.ok) throw new Error(`WASM HTTP ${response.status}`);
  const wasm = await response.arrayBuffer();
  const messages = [];
  const entries = [{ path: "smoke/smoke.parquet", file },
    { path: "unrelated/unrelated.parquet", file: unrelated }];
  if (process.argv.includes("--tpch")) {
    const tables = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"];
    await promisify(execFile)("duckdb", ["-c", "LOAD tpch; CALL dbgen(sf=0.001); " + tables.map(
      table => `COPY ${table} TO '${directory}/${table}.parquet' (FORMAT PARQUET)`
    ).join("; ")]);
    for (const table of tables) {
      const payload = await readFile(`${directory}/${table}.parquet`);
      const tableFile = new File([payload], `${table}.parquet`);
      tableFile.slice = (start, end) => payload.subarray(start, end);
      entries.push({ path: `${table}/${table}.parquet`, file: tableFile });
    }
  }
  const collectors = new Map();
  const receive = createEngineRuntime({
    loadModule: async () => ({ ...module, default: () => module.default({ module_or_path: wasm }) }),
    postMessage: message => {
      if (message.type === "result-batch") {
        let collector = collectors.get(message.requestId);
        if (!collector) {
          collector = new ResultCollector(async buffer => tableFromIPC(new Uint8Array(buffer)),
            assembleArrowTables);
          collectors.set(message.requestId, collector);
        }
        void collector.append(message).then(() => receive({ type: "result-ack", requestId: message.requestId,
          sequence: message.sequence, accepted: true }));
      } else {
        if (message.type === "result") {
          message.table = collectors.get(message.requestId).finish(message.transport).table;
          collectors.get(message.requestId).release(); collectors.delete(message.requestId);
        }
        messages.push(message);
      }
    },
  });
  await receive({ type: "load", requestId: 1, directoryName: "validation",
    entries });
  const ready = messages.find(message => message.type === "ready");
  if (!ready) throw new Error(JSON.stringify(messages.at(-1)));
  assert.equal(ready.ioStatistics.bytesRead, 0);
  await receive({ type: "query", requestId: 2,
    query: "FROM smoke |> WHERE id > 1 |> AGGREGATE sum(amount) AS total, count(*) AS n" });
  const result = messages.find(message => message.type === "result");
  if (!result) throw new Error(JSON.stringify(messages.at(-1)));
  const table = result.table;
  assert.equal(formatCell(table.getChildAt(0).get(0), table.schema.fields[0].type), "50");
  assert.equal(String(table.getChildAt(1).get(0)), "2");
  assert.ok(result.ioStatistics.bytesRead > 0);
  assert.ok(result.ioStatistics.bytesRead < bytes.byteLength, "projection must avoid unused payload");
  for (let requestId = 3; requestId <= 5; requestId += 1) {
    await receive({ type: "query", requestId, query: "FROM smoke |> AGGREGATE count(*) AS n" });
    const next = messages.find(message => message.type === "result" && message.requestId === requestId);
    if (!next) throw new Error(JSON.stringify(messages.at(-1)));
    assert.equal(String(next.table.getChildAt(0).get(0)), "3");
  }
  if (process.argv.includes("--tpch")) {
    const queries = await (await fetch(new URL("/queries/tpch-pipe.json", origin))).json();
    const failures = [];
    for (const [index, { id, query }] of queries.entries()) {
      const requestId = 10 + index;
      console.log(`Executing ${id} on the existing engine`);
      await receive({ type: "query", requestId, query });
      const message = messages.find(value => value.requestId === requestId && ["result", "error"].includes(value.type));
      if (message?.type !== "result") {
        failures.push(id);
        console.log(JSON.stringify(message ?? messages.at(-1)));
        continue;
      }
      console.log(JSON.stringify({ id, rows: message.table.numRows, engineMs: message.engineMs }));
    }
    if (failures.length) {
      console.error(`Query errors: ${failures.join(", ")}`);
      process.exitCode = 1;
    }
  }
  console.log(JSON.stringify({ registration: ready.ioStatistics,
    expected: { total: 50, n: 2 }, fileBytes: bytes.byteLength,
    io: result.ioStatistics, engineMs: result.engineMs }));
}

const directory = await mkdtemp("/private/tmp/vein-browser-");
try {
  await validate(directory);
} catch (error) {
  console.error(String(error.message).slice(0, 600));
  process.exitCode = 1;
} finally {
  await rm(directory, { recursive: true, force: true });
}
