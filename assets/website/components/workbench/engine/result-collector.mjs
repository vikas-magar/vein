/** Decoded Arrow batches retain transferred column buffers; never concatenate payload bytes. */
export class ResultCollector {
  constructor(decode, combine) {
    this.decode = decode; this.combine = combine; this.tables = []; this.byteLength = 0;
  }
  async append(data) {
    if (data.sequence !== this.tables.length || !(data.buffer instanceof ArrayBuffer)) {
      throw new Error("Invalid result packet sequence or buffer");
    }
    const table = await this.decode(data.buffer);
    this.tables.push(table); this.byteLength += data.buffer.byteLength;
  }
  finish(transport) {
    if (!this.tables.length || transport?.packets !== this.tables.length || transport.byteLength !== this.byteLength) {
      throw new Error("Incomplete result stream");
    }
    return { table: this.combine(this.tables), byteLength: this.byteLength };
  }
  release() { this.tables.length = 0; this.byteLength = 0; }
}
