import { Actions, DockLocation, Model, TabNode, TabSetNode } from "flexlayout-react";
import type { Action, IJsonModel } from "flexlayout-react";
import type { Documents } from "../workspace/documents";

export const defaultLayout: IJsonModel = {
  global: { tabEnableFloat: false, tabEnableRename: true, tabEnableClose: true,
    tabSetEnableDeleteWhenEmpty: false, tabSetMinWidth: 160, tabSetMinHeight: 100 },
  borders: [],
  layout: { type: "row", children: [
    { type: "tabset", id: "explorer", weight: 20, enableMaximize: false,
      children: [{ type: "tab", id: "data", name: "Data", component: "explorer", enableClose: false, enableRename: false },
        { type: "tab", name: "Examples", component: "examples", enableClose: false, enableRename: false },
        { type: "tab", name: "History", component: "history", enableClose: false, enableRename: false }] },
    { type: "row", weight: 80, children: [
      { type: "tabset", id: "editors", weight: 62, children: [] },
      { type: "tabset", id: "output", weight: 38, children: [
        { type: "tab", name: "Results", component: "results", enableClose: false, enableRename: false },
        { type: "tab", name: "Messages", component: "messages", enableClose: false, enableRename: false },
        { type: "tab", name: "Details", component: "details", enableClose: false, enableRename: false }] },
    ] },
  ] },
};

/** Layout changes never construct or dispose an engine. */
export class WorkspaceLayout {
  readonly model: Model;
  private active = "";
  private listeners = new Set<() => void>();
  constructor(private documents: Documents, saved?: IJsonModel, compact = false) {
    const initial = compact && !saved ? {
      ...defaultLayout,
      global: { ...defaultLayout.global, rootOrientationVertical: true },
      borders: [{ type: "border" as const, location: "left", size: 230, selected: -1,
        children: defaultLayout.layout.children![0].children }],
      layout: defaultLayout.layout.children![1],
    } as IJsonModel : defaultLayout;
    this.model = Model.fromJson(saved ?? initial);
    const orphaned: string[] = [];
    this.model.visitNodes(node => {
      if (node instanceof TabNode && node.getComponent() === "query") {
        const id = node.getConfig()?.documentId;
        if (!this.documents.get(id)) orphaned.push(node.getId());
      }
    });
    orphaned.forEach(id => this.model.doAction(Actions.deleteTab(id)));
    this.active = this.selectedDocument();
  }
  getActive = () => this.active;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener); return () => { this.listeners.delete(listener); };
  };
  open(name: string, text: string, id?: string) {
    const documentId = this.documents.open(name, text, id);
    const nodeId = `document:${documentId}`;
    if (this.model.getNodeById(nodeId)) this.model.doAction(Actions.selectTab(nodeId));
    else {
      let target = this.model.getNodeById("editors");
      this.model.visitNodes(node => {
        if (node instanceof TabNode && node.getComponent() === "query") target = node.getParent();
      });
      const targetId = target?.getId() ?? this.model.getActiveTabset()?.getId();
      if (targetId) this.model.doAction(Actions.addTab({ type: "tab", id: nodeId, name,
        component: "query", config: { documentId } }, targetId, DockLocation.CENTER, -1, true));
    }
    this.activate(documentId);
    return documentId;
  }
  /** Output selection must not change which query owns the result or steal editor focus. */
  showResults() {
    let target: TabNode | undefined;
    this.model.visitNodes(node => {
      if (node instanceof TabNode && node.getComponent() === "results") target = node;
    });
    if (target) this.model.doAction(Actions.selectTab(target.getId()));
  }
  selectDocument(id: string) {
    const node = this.model.getNodeById(`document:${id}`);
    if (!(node instanceof TabNode) || !this.documents.get(id)) return;
    this.model.doAction(Actions.selectTab(node.getId()));
    this.activate(id);
  }
  changed = (_: Model, action: Action) => {
    const node = this.model.getNodeById(action.data.tabNode ?? action.data.node);
    if (node instanceof TabNode && node.getComponent() === "query") {
      this.activate(node.getConfig().documentId);
      if (action.type === Actions.RENAME_TAB) this.documents.rename(node.getConfig().documentId, node.getName());
    }
    if (action.type === Actions.SET_ACTIVE_TABSET && node instanceof TabSetNode) {
      const selected = node.getSelectedNode();
      if (selected instanceof TabNode && selected.getComponent() === "query") this.activate(selected.getConfig().documentId);
    }
    if (action.type === Actions.DELETE_TAB) {
      const id = String(action.data.node).replace(/^document:/, "");
      this.documents.close(id);
      if (id === this.active) {
        this.activate(this.selectedDocument());
      }
    }
  };
  private selectedDocument() {
    const active = this.model.getActiveTabset()?.getSelectedNode();
    if (active instanceof TabNode && active.getComponent() === "query") return active.getConfig().documentId;
    let id = "";
    this.model.visitNodes(node => {
      if (node instanceof TabSetNode) {
        const selected = node.getSelectedNode();
        if (selected instanceof TabNode && selected.getComponent() === "query") id = selected.getConfig().documentId;
      }
    });
    return id;
  }
  private activate(id: string) {
    if (id === this.active) return;
    this.active = id; this.listeners.forEach(listener => listener());
  }
}
