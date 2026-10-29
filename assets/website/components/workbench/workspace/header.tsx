import Link from "next/link";
import { Button, Dialog, DialogTrigger, Heading, Modal, ModalOverlay, Input, SearchField, Label } from "react-aria-components";
import { useRef, useState } from "react";
import { assetUrl } from "../assets.mjs";
import { useActiveDocument, useWorkspace } from "./hooks";
import Icon from "../icon";
import Preferences from "./preferences";
import InstallCli from "./install";
import Wordmark from "../../wordmark";
import MobileMenu, { type WorkspacePanel } from "./mobile-menu";

export default function WorkspaceHeader() {
  const [search, setSearch] = useState("");
  const [panel, setPanel] = useState<WorkspacePanel | null>(null);
  const menuButton = useRef<HTMLButtonElement>(null);
  function changePanel(target: WorkspacePanel, open: boolean) {
    setPanel(open ? target : null);
    if (!open) requestAnimationFrame(() => {
      if (menuButton.current?.getClientRects().length) menuButton.current.focus();
    });
  }
  const { layout, openDirectory, session } = useWorkspace();
  const id = useActiveDocument();
  const actions = [
    { name: "New query", run: () => layout.open("Untitled.vein", "") },
    { name: "Open data directory", run: openDirectory },
    { name: "Stop execution", run: session.stop },
    { name: "Release current result", run: () => session.releaseResult(id) },
  ];
  return <header className="ide-header">
    <Link href="/" className="ide-brand"><img src={assetUrl("vein.svg")} width={30} height={30} alt="" /><Wordmark /></Link>
    <span className="ide-tagline">Query your data. Keep it local.</span>
    <InstallCli />
    <span className="ide-toolbar-spacer" />
    <DialogTrigger isOpen={panel === "commands"} onOpenChange={open => changePanel("commands", open)}>
      <Button className="ide-command"><Icon name="search" size={13} /><span>Commands</span></Button>
      <ModalOverlay className="ide-modal-overlay"><Modal className="ide-modal"><Dialog>
        {({ close }) => <><Heading slot="title">Workspace commands</Heading>
          <SearchField className="ide-search" value={search} onChange={setSearch}><Label className="ide-sr-only">Find a command</Label><Input autoFocus placeholder="Find a command…" /></SearchField>
          {actions.filter(action => action.name.toLowerCase().includes(search.toLowerCase())).map(action =>
            <Button key={action.name} className="ide-command-item" onPress={() => { action.run(); close(); }}>{action.name}</Button>)}
          <Button className="ide-tool" onPress={close}>Close</Button></>}
      </Dialog></Modal></ModalOverlay>
    </DialogTrigger>
    <Link className="ide-docs" href="/docs/">Documentation<Icon name="arrow" size={13} /></Link>
    <Preferences isOpen={panel === "settings"} onOpenChange={open => changePanel("settings", open)} />
    <MobileMenu triggerRef={menuButton} onOpen={setPanel} />
  </header>;
}
