import { createEngineRuntime } from "./worker/engine-runtime.mjs";

const receive = createEngineRuntime({
  loadModule: url => import(url),
  postMessage: (message, transfer = []) => self.postMessage(message, transfer),
});

self.addEventListener("message", ({ data }) => receive(data));
