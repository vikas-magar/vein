import { StateField, type Text } from "@codemirror/state";
import { Decoration, EditorView, WidgetType } from "@codemirror/view";
import type { QueryStatement } from "./statements";
import { createStatementIndex, updateStatementIndex, type StatementIndex } from "./statement-index";

export interface QueryControlHost extends QueryStatement { element: HTMLDivElement; id: number }

class QueryControlWidget extends WidgetType {
  constructor(readonly element: HTMLDivElement) { super(); }
  toDOM() { return this.element; }
  ignoreEvent() { return true; }
  eq(other: QueryControlWidget) { return this.element === other.element; }
}

function createControls(doc: Text, index: StatementIndex, previous: QueryControlHost[] = []) {
  const elements = new Map(previous.map(host => [host.id, host.element]));
  const hosts: QueryControlHost[] = [];
  for (const segment of index.segments) if (segment.statement) {
    hosts.push({ ...segment.statement, id: segment.id, number: hosts.length + 1,
      element: elements.get(segment.id) ?? document.createElement("div") });
  }
  const decorations = Decoration.set(hosts.map(host => {
    host.element.className = "ide-query-control-host";
    return Decoration.widget({ widget: new QueryControlWidget(host.element), block: true, side: 1 })
      .range(doc.lineAt(host.to).to);
  }), true);
  return { hosts, decorations, index };
}

/** State-backed block widgets reserve real document space and follow editor scrolling. */
export const queryControls = StateField.define({
  create: state => createControls(state.doc, createStatementIndex(state.doc)),
  update: (value, transaction) => transaction.docChanged
    ? createControls(transaction.state.doc, updateStatementIndex(value.index, transaction), value.hosts) : value,
  provide: field => EditorView.decorations.from(field, value => value.decorations),
});
