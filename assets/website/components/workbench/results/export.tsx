import { memo } from "react";
import { Button } from "react-aria-components";
import type { QueryResult } from "../workspace/contracts";
import Icon from "../icon";

/** Export only on request; rendering the footer never reads or copies result columns. */
export default memo(function ExportResult({ result, name }: { result: QueryResult; name: string }) {
  async function download() {
    const { tableToIPC } = await import("apache-arrow");
    const ipc = tableToIPC(result.table, "stream");
    const bytes = ipc.buffer instanceof ArrayBuffer
      ? new Uint8Array(ipc.buffer, ipc.byteOffset, ipc.byteLength) : new Uint8Array(ipc);
    const url = URL.createObjectURL(new Blob([bytes], { type: "application/vnd.apache.arrow.stream" }));
    const anchor = document.createElement("a"); anchor.href = url;
    anchor.download = `${name}.arrow`; anchor.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  return <Button className="ide-tool" onPress={download}><Icon name="download" size={13} />Arrow</Button>;
});
