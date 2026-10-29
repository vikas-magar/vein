const registries = new Map();
let nextId = 1;

const emptyStatistics = () => ({ readCalls: 0, requestedBytes: 0, bytesRead: 0, maxReadBytes: 0 });

export function createRegistry() {
  if (nextId > 0xffffffff) throw new Error("Browser file registry identifier space exhausted");
  const id = nextId++;
  registries.set(id, { files: new Map(), statistics: emptyStatistics() });
  return id;
}

function registry(id) {
  const value = registries.get(id);
  if (!value) throw new Error("The browser file registry is closed");
  return value;
}

export function registerFile(id, path, file) {
  const value = registry(id);
  if (value.files.has(path)) throw new Error(`Duplicate file: ${path}`);
  if (!file || typeof file.slice !== "function" || !Number.isSafeInteger(file.size) || file.size < 0) {
    throw new Error("A valid browser File object is required");
  }
  value.files.set(path, file);
}

export function readInto(id, path, offset, output) {
  const { files, statistics } = registry(id);
  const file = files.get(path);
  if (!file) throw new Error(`File is not registered: ${path}`);
  const end = offset + output.byteLength;
  if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(end) || end > file.size) {
    throw new Error(`Invalid byte range for ${path}`);
  }
  if (!output.byteLength) return;
  if (typeof FileReaderSync !== "function") throw new Error("Browser file reads require a dedicated Web Worker with FileReaderSync support");
  statistics.readCalls += 1;
  statistics.requestedBytes += output.byteLength;
  const buffer = new FileReaderSync().readAsArrayBuffer(file.slice(offset, end));
  if (buffer.byteLength !== output.byteLength) throw new Error(`Incomplete byte range for ${path}`);
  // output is a borrowed view of the Rust destination in WASM linear memory.
  output.set(new Uint8Array(buffer));
  statistics.bytesRead += buffer.byteLength;
  statistics.maxReadBytes = Math.max(statistics.maxReadBytes, buffer.byteLength);
}

export function takeStatistics(id) {
  const value = registry(id);
  const statistics = value.statistics;
  value.statistics = emptyStatistics();
  return statistics;
}

export function dropRegistry(id) { registries.delete(id); }
