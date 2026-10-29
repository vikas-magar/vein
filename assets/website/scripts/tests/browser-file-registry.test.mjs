import assert from "node:assert/strict";
import test from "node:test";
import { createRegistry, registerFile, readInto, takeStatistics, dropRegistry } from "../../../../crates/wasm/src/browser/files/registry.mjs";

test("browser registration reads zero bytes and range reads fill only the supplied destination", () => {
  const previous = globalThis.FileReaderSync;
  const bytes = new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  const ranges = [];
  const id = createRegistry();
  try {
    globalThis.FileReaderSync = class {
      readAsArrayBuffer({ start, end }) { ranges.push([start, end]); return bytes.slice(start, end).buffer; }
    };
    const file = { size: bytes.length, slice: (start, end) => ({ start, end }),
      arrayBuffer() { throw new Error("Whole-file reading is forbidden"); } };
    registerFile(id, "part/file.parquet", file);
    assert.deepEqual(takeStatistics(id), { readCalls: 0, requestedBytes: 0, bytesRead: 0, maxReadBytes: 0 });
    const destination = new Uint8Array(7).fill(255);
    readInto(id, "part/file.parquet", 3, destination.subarray(2, 5));
    assert.deepEqual([...destination], [255, 255, 3, 4, 5, 255, 255]);
    assert.deepEqual(ranges, [[3, 6]]);
    assert.deepEqual(takeStatistics(id), { readCalls: 1, requestedBytes: 3, bytesRead: 3, maxReadBytes: 3 });
    assert.equal(takeStatistics(id).readCalls, 0);
  } finally { globalThis.FileReaderSync = previous; dropRegistry(id); }
});

test("registry lifetimes, duplicate files and invalid ranges fail without silent reads", () => {
  const id = createRegistry();
  const file = { size: 10, slice() { throw new Error("Must not read invalid ranges"); } };
  registerFile(id, "file", file);
  assert.throws(() => registerFile(id, "file", file), /Duplicate/);
  assert.throws(() => readInto(id, "missing", 0, new Uint8Array(1)), /not registered/);
  for (const offset of [-1, 9, NaN, 0.5, Number.MAX_SAFE_INTEGER]) {
    assert.throws(() => readInto(id, "file", offset, new Uint8Array(2)), /Invalid byte range/);
  }
  dropRegistry(id);
  assert.throws(() => takeStatistics(id), /closed/);
});
