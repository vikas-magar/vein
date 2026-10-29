import assert from "node:assert/strict";
import test from "node:test";
import { tableFiles } from "../../public/worker/table-files.mjs";

function file(path) {
  return { name: path.split("/").at(-1), webkitRelativePath: path, arrayBuffer() { throw new Error("Payload must not be read during discovery"); } };
}

test("directory selection normalizes multiple table folders without reading file contents", () => {
  const part = file("tpch/part/part.1.parquet");
  const supplier = file("tpch/supplier/supplier.1.PARQUET");
  const entries = tableFiles([supplier, file("tpch/notes.txt"), part]);
  assert.deepEqual(entries.map(entry => entry.path), ["part/part.1.parquet", "supplier/supplier.1.PARQUET"]);
  assert.equal(entries[0].file, part);
  assert.equal(entries[1].file, supplier);
});

test("wrong roots, flat files, traversal and duplicate entries have actionable errors", () => {
  assert.throws(() => tableFiles([file("tpch/part.parquet")]), /parent data directory/);
  assert.throws(() => tableFiles([file("tpch/part/a.parquet"), file("other/part/b.parquet")]), /parent data directory/);
  assert.throws(() => tableFiles([file("tpch/../a.parquet")]), /Invalid directory entry/);
  const duplicate = file("tpch/part/a.parquet");
  assert.throws(() => tableFiles([duplicate, duplicate]), /Duplicate/);
  assert.throws(() => tableFiles([file("tpch/notes.txt")]), /No Parquet files/);
});
