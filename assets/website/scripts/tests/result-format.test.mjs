import assert from "node:assert/strict";
import test from "node:test";
import { Decimal, DateDay, makeData, makeVector } from "apache-arrow";
import { formatCell } from "../../components/workbench/results/format.mjs";

function decimal(value, scale, width = 128) {
  const words = new Uint32Array(width / 32);
  let bits = BigInt.asUintN(width, value);
  for (let index = 0; index < words.length; index += 1) { words[index] = Number(bits & 0xffffffffn); bits >>= 32n; }
  const type = new Decimal(scale, width === 128 ? 38 : 76, width);
  return { type, value: makeVector(makeData({ type, data: words, length: 1 })).get(0) };
}

test("Arrow decimals retain scale, sign and full 128/256-bit precision when displayed", () => {
  for (const [raw, scale, width, expected] of [
    [12345n, 2, 128, "123.45"], [-12345n, 2, 128, "-123.45"], [5n, 4, 128, "0.0005"],
    [1234567890123456789012345678901234567890n, 2, 256, "12345678901234567890123456789012345678.90"],
  ]) {
    const { value, type } = decimal(raw, scale, width);
    assert.equal(formatCell(value, type), expected);
  }
});

test("dates, large integers, NULL and nested bigints render without precision loss or crashes", () => {
  const type = new DateDay();
  const value = makeVector(makeData({ type, data: new Int32Array([0]), length: 1 })).get(0);
  assert.equal(formatCell(value, type), "1970-01-01");
  assert.equal(formatCell(9223372036854775807n), "9223372036854775807");
  assert.equal(formatCell(null), "NULL");
  assert.equal(formatCell({ count: 9007199254740993n }), '{"count":"9007199254740993"}');
});
