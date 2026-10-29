import { exactArrayBuffer } from "./arrow-buffer.mjs";

/** One transferred packet awaits acknowledgement; no result-wide IPC buffer is retained here. */
export class ResultDelivery {
  constructor(postMessage, now, packetBytes = 1024 * 1024) {
    this.postMessage = postMessage; this.now = now; this.packetBytes = packetBytes; this.pending = null;
  }
  acknowledge(data) {
    if (data.type !== "result-ack") return false;
    const pending = this.pending;
    if (pending && data.requestId === pending.requestId && data.sequence === pending.sequence) {
      clearTimeout(pending.timer); this.pending = null;
      if (data.accepted) pending.resolve();
      else pending.reject(new Error(data.message || "Result consumer rejected a packet"));
    }
    return true;
  }
  async run(engine, data) {
    let packets = 0, byteLength = 0, maxPacketBytes = 0, acknowledgementMs = 0;
    const started = this.now();
    try {
      await engine.query(data.query, async ipc => {
        const buffer = exactArrayBuffer(ipc);
        if (buffer.byteLength > this.packetBytes) throw new Error("Engine exceeded its IPC packet limit");
        const sequence = packets++;
        byteLength += buffer.byteLength; maxPacketBytes = Math.max(maxPacketBytes, buffer.byteLength);
        const waitStarted = this.now();
        await new Promise((resolve, reject) => {
          const timer = setTimeout(() => {
            this.pending = null; reject(new Error("Result acknowledgement timed out"));
          }, 30_000);
          this.pending = { resolve, reject, timer, requestId: data.requestId, sequence };
          this.postMessage({ type: "result-batch", requestId: data.requestId, sequence, buffer }, [buffer]);
        });
        acknowledgementMs += this.now() - waitStarted;
      }, this.packetBytes);
      return { engineMs: Math.max(0, this.now() - started - acknowledgementMs),
        transport: { packets, byteLength, maxPacketBytes, acknowledgementMs } };
    } finally {
      if (this.pending) { clearTimeout(this.pending.timer); this.pending = null; }
    }
  }
}
