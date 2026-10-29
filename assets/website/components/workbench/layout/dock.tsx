import { Actions, Layout, TabNode } from "flexlayout-react";
import type { Action } from "flexlayout-react";
import { useState } from "react";
import { Button, Dialog, Heading, Modal, ModalOverlay } from "react-aria-components";
import { useDocument, useWorkspace } from "../workspace/hooks";
import QueryPanel from "../query/panel";
import ExplorerPanel from "../explorer/panel";
import ResultsPanel from "../results/panel";
import DetailsPanel from "../results/details";

function DocumentTitle({ id }: { id: string }) {
  const document = useDocument(id);
  return <span>{document?.name ?? "Query"}{document && document.text !== document.original && <span className="ide-dirty" aria-label="Unsaved changes">●</span>}</span>;
}

export default function WorkspaceDock() {
  const { layout, session } = useWorkspace();
  const [closing, setClosing] = useState<Action | null>(null);
  function factory(node: TabNode) {
    switch (node.getComponent()) {
      case "query": return <QueryPanel documentId={node.getConfig().documentId} />;
      case "explorer": case "examples": case "history":
        return <ExplorerPanel mode={node.getComponent() as "explorer" | "examples" | "history"} />;
      case "results": return <ResultsPanel />;
      case "messages": return <DetailsPanel messages />;
      case "details": return <DetailsPanel />;
      default: return <div className="ide-empty">Panel unavailable.</div>;
    }
  }
  return <div className="ide-dock"><Layout model={layout.model} factory={factory} supportsPopout={false}
    onAction={action => {
      if (action.type === Actions.DELETE_TAB) {
        const node = layout.model.getNodeById(action.data.node);
        const document = node instanceof TabNode ? session.documents.get(node.getConfig()?.documentId) : undefined;
        if (document && (document.execution === "running" || document.text !== document.original)) { setClosing(action); return; }
      }
      return action;
    }}
    onModelChange={(model, action) => layout.changed(model, action)}
    onRenderTab={(node, values) => {
      if (node.getComponent() === "query") values.content = <DocumentTitle id={node.getConfig().documentId} />;
    }}
    onRenderTabSet={(node, values) => {
      if (node.getId() === "editors" || ("getChildren" in node && node.getChildren().some(child => child instanceof TabNode && child.getComponent() === "query"))) {
        values.stickyButtons.push(<Button key="new" className="ide-new-tab" aria-label="New query"
          onPress={() => layout.open("Untitled.vein", "")}>+</Button>);
      }
    }} />
    <ModalOverlay className="ide-modal-overlay" isOpen={closing !== null} onOpenChange={open => { if (!open) setClosing(null); }}>
      <Modal className="ide-modal"><Dialog role="alertdialog">
        <Heading slot="title">Close this query?</Heading>
        <p>The draft or execution belongs to this tab. Closing it releases its editor state and retained results.</p>
        <div className="ide-confirm-actions"><Button className="ide-tool" onPress={() => setClosing(null)}>Keep editing</Button>
          <Button className="ide-primary" onPress={() => {
            if (closing) { layout.model.doAction(closing); layout.changed(layout.model, closing); }
            setClosing(null);
          }}>Close query</Button></div>
      </Dialog></Modal>
    </ModalOverlay>
  </div>;
}
