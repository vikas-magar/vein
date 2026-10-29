import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const sourcePath = resolve(scriptDirectory, "../../sql/tpch/pipe/queries.pipe");
const outputPath = resolve(scriptDirectory, "../public/queries/tpch-pipe.json");
const source = await readFile(sourcePath, "utf8");
const queries = parseQueries(source);

if (queries.length !== 22 || queries.some((query, index) => query.id !== `Q${index + 1}`)) {
  throw new Error(`Expected canonical TPC-H Q1–Q22; parsed ${queries.length} queries.`);
}

await mkdir(dirname(outputPath), { recursive: true });
await writeFile(outputPath, `${JSON.stringify(queries, null, 2)}\n`);
console.info(`Exported ${queries.length} canonical TPC-H Pipe queries.`);

function parseQueries(text) {
  const entries = [];
  let current = null;
  const flush = () => {
    if (!current) return;
    current.query = current.lines.join("\n").trim();
    delete current.lines;
    if (!current.id || !current.title || !current.query || !current.columns.length) {
      throw new Error(`Incomplete query metadata: ${JSON.stringify(current)}`);
    }
    entries.push(current);
    current = null;
  };

  for (const line of text.split(/\r?\n/)) {
    const name = line.match(/^--\s*name:\s*(q\d+)-(.+)$/i);
    if (name) {
      flush();
      current = { id: name[1].toUpperCase(), slug: line.split(":")[1].trim(), title: titleCase(name[2]), columns: [], lines: [] };
      continue;
    }
    if (!current) continue;
    const columns = line.match(/^--\s*columns:\s*(.+)$/i);
    if (columns) current.columns = columns[1].split(",").map(value => value.trim());
    else if (!line.trimStart().startsWith("--")) current.lines.push(line);
  }
  flush();
  return entries;
}

function titleCase(value) {
  return value.split("-").map(word => word[0].toUpperCase() + word.slice(1)).join(" ");
}
