import { memo, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { EditorState, StateEffect } from "@codemirror/state";
import { EditorView, drawSelection, highlightActiveLine, keymap, lineNumbers } from "@codemirror/view";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { autocompletion, closeCompletion } from "@codemirror/autocomplete";
import { pipeInput } from "./completion";
import { language, syntax, theme } from "./editor-theme";
import { useWorkspace } from "../workspace/hooks";
import { queryControls, type QueryControlHost } from "./controls-extension";
import QueryControls from "./controls";
import { queryAt } from "./statements";
import { semanticCompletion } from "./semantic-completion";
import { queryDiagnostics } from "./diagnostics";
import { forceLinting } from "@codemirror/lint";
import type { EngineDiagnostic } from "./inspection-types";
import Problems from "./problems";

/** CodeMirror owns text transactions; React supplies the embedded controls and query-level messages. */
export default memo(function QueryEditor({ documentId }: { documentId: string }) {
  const root = useRef<HTMLDivElement>(null);
  const editor = useRef<EditorView | null>(null);
  const { session, run } = useWorkspace();
  const [hosts, setHosts] = useState<QueryControlHost[]>([]);
  const [errors, setErrors] = useState<EngineDiagnostic[]>([]);
  useEffect(() => {
    const document = session.documents.get(documentId);
    if (!root.current || !document) return;
    const extensions = [language, syntax, theme, lineNumbers(), drawSelection(), highlightActiveLine(), history(),
      autocompletion({ override: [semanticCompletion(session)] }), pipeInput, queryDiagnostics(session, setErrors),
      queryControls,
      keymap.of([{ key: "Mod-Enter", run: view => {
        const query = queryAt(view.state.doc.toString(), view.state.selection.main.head);
        if (query) run(documentId, query.query, query.number);
        return true;
      } },
        ...defaultKeymap, ...historyKeymap, indentWithTab]),
      EditorView.contentAttributes.of({ "aria-label": `${document.name} query editor` }),
      EditorView.updateListener.of(update => {
        session.documents.editorStates.set(documentId, update.state);
        if (update.docChanged) {
          setErrors([]);
          session.documents.edit(documentId, update.state.doc.toString());
          setHosts(update.state.field(queryControls).hosts);
        }
      }),
    ];
    const saved = session.documents.editorStates.get(documentId);
    const state = saved instanceof EditorState
      ? saved.update({ effects: StateEffect.reconfigure.of(extensions) }).state
      : EditorState.create({ doc: document.text, extensions });
    const view = new EditorView({ state, parent: root.current });
    editor.current = view;
    setHosts(view.state.field(queryControls).hosts);
    let ready = session.snapshot.phase === "ready";
    const unsubscribe = session.subscribe(() => {
      if (session.snapshot.phase === "running") closeCompletion(view);
      const next = session.snapshot.phase === "ready";
      if (next && !ready) forceLinting(view);
      ready = next;
    });
    const measure = () => view.requestMeasure();
    window.addEventListener("vein:editor-font-change", measure);
    return () => {
      unsubscribe();
      if (session.documents.get(documentId)) session.documents.editorStates.set(documentId, view.state);
      else session.documents.editorStates.delete(documentId);
      window.removeEventListener("vein:editor-font-change", measure);
      view.destroy();
      editor.current = null;
    };
  }, [documentId, session, run]);
  return <><div className="ide-editor-shell"><div className="ide-editor" ref={root} />
    <Problems errors={errors} onNavigate={(from, to) => {
      const view = editor.current;
      if (!view || to > view.state.doc.length) return;
      view.dispatch({ selection: { anchor: from, head: to }, scrollIntoView: true });
      view.focus();
    }} />
  </div>{hosts.map(host => createPortal(
    <QueryControls documentId={documentId} query={host.query} statementNumber={host.number} />,
    host.element, host.id,
  ))}</>;
});
