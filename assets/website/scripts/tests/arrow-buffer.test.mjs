import assert from "node:assert/strict";
import test from "node:test";
import { exactArrayBuffer } from "../../public/worker/arrow-buffer.mjs";

test("transfers only the bytes covered by a typed-array view", () => {
  const backing = Uint8Array.from([90, 91, 10, 11, 12, 92]);
  const view = backing.subarray(2, 5);
  const transferred = new Uint8Array(exactArrayBuffer(view));

  assert.deepEqual([...transferred], [10, 11, 12]);
  assert.equal(transferred.byteOffset, 0);
  assert.equal(transferred.byteLength, 3);
});

test("reuses an exact Arrow backing buffer without copying", () => {
  const backing = Uint8Array.from([1, 2, 3]);

  assert.equal(exactArrayBuffer(backing), backing.buffer);
});

test("preserves empty Arrow result views", () => {
  const view = new Uint8Array(new ArrayBuffer(8), 4, 0);
  const transferred = exactArrayBuffer(view);

  assert.equal(transferred.byteLength, 0);
});
