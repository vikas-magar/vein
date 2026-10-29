import { DataType } from "apache-arrow";

export function formatBytes(bytes) {
  if (bytes < 1_000_000) return `${(bytes / 1_000).toFixed(1)} KB`;
  if (bytes < 1_000_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
  return `${(bytes / 1_000_000_000).toFixed(2)} GB`;
}

export function formatCell(value, type) {
  if (value == null) return "NULL";
  if (type && DataType.isDecimal(type)) return scaledDecimal(decimalInteger(value), type.scale);
  if (value instanceof Date) return value.toISOString();
  if (type && (DataType.isDate(type) || DataType.isTimestamp(type))) {
    const date = new Date(Number(value));
    if (Number.isFinite(date.getTime())) return DataType.isDate(type) ? date.toISOString().slice(0, 10) : date.toISOString();
  }
  if (typeof value !== "object") return String(value);
  try { return JSON.stringify(value, (_, item) => typeof item === "bigint" ? String(item) : item); }
  catch { return String(value); }
}

function decimalInteger(value) {
  if (!ArrayBuffer.isView(value) || value.BYTES_PER_ELEMENT !== 4) return String(value);
  // Arrow stores decimal words little-endian in two's complement. Avoid a
  // Number conversion and support both Decimal128 and Decimal256 exactly.
  let integer = 0n;
  for (let index = value.length - 1; index >= 0; index -= 1) {
    integer = (integer << 32n) | BigInt(value[index] >>> 0);
  }
  return BigInt.asIntN(value.length * 32, integer).toString();
}

function scaledDecimal(raw, scale) {
  const negative = raw.startsWith("-");
  let digits = negative ? raw.slice(1) : raw;
  if (scale < 0) digits += "0".repeat(-scale);
  if (scale > 0) {
    digits = digits.padStart(scale + 1, "0");
    digits = `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
  }
  return `${negative ? "-" : ""}${digits}`;
}
