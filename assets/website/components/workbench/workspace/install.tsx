import { useState } from "react";
import { Button, Dialog, DialogTrigger, Heading, Modal, ModalOverlay } from "react-aria-components";
import { assetUrl } from "../assets.mjs";

export default function InstallCli() {
  const [command, setCommand] = useState("");
  const [copyState, setCopyState] = useState("");
  return <DialogTrigger>
    <Button className="ide-install" onPress={() => {
      setCommand(`curl -fsSL ${new URL(assetUrl("install.sh"), window.location.origin).href} | sh`);
      setCopyState("");
    }}>Install CLI<span aria-hidden="true">↗</span></Button>
    <ModalOverlay className="ide-modal-overlay"><Modal className="ide-modal ide-install-modal"><Dialog>
      {({ close }) => <><Heading slot="title">Run Vein from your terminal</Heading>
        <p>Install the latest CLI release for macOS ARM64 or Linux x86-64 (glibc).</p>
        <pre className="ide-install-command"><code>{command}</code></pre>
        <p>The installer verifies SHA-256 checksums and installs into <code>~/.local/bin</code>. No root access is needed.</p>
        <p className="ide-footnote">Requires a published public GitHub release. This development website does not publish CLI binaries.</p>
        <div className="ide-confirm-actions"><a className="ide-tool" href={assetUrl("install.sh")} target="_blank" rel="noreferrer">View installer</a>
          <Button className="ide-tool" onPress={close}>Close</Button>
          <Button className="ide-primary" onPress={async () => {
            try { await navigator.clipboard.writeText(command); setCopyState("Copied"); }
            catch { setCopyState("Select and copy the command above"); }
          }}>{copyState || "Copy command"}</Button></div>
      </>}
    </Dialog></Modal></ModalOverlay>
  </DialogTrigger>;
}
