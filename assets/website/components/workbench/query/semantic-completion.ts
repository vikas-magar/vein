import { snippetCompletion, type CompletionContext, type CompletionResult } from "@codemirror/autocomplete";
import type { WorkspaceSession } from "../workspace/session";
import { pipeCompletion } from "./completion";
import { inLiteral, inspectionContext, quoteIdentifier } from "./inspection-context";
import type { Inspection } from "./inspection-types";

export function semanticCompletion(session: WorkspaceSession) {
  return async (context: CompletionContext): Promise<CompletionResult | null> => {
    if (inLiteral(context.state, context.pos)) return null;
    const base = pipeCompletion(context);
    const word = context.matchBefore(/[\p{L}\p{N}_$]*/u);
    if (!word || (!context.explicit && word.from === word.to)) return base;
    const scope = inspectionContext(context.state, context.pos);
    if (!scope) return base;
    if (/\b(?:FROM|JOIN)\s+[\p{L}\p{N}_$]*$/iu.test(scope.before)) {
      return { from: word.from, options: (session.snapshot.dataset?.tables ?? []).map(name => ({
        label: name, type: "class", detail: "table", apply: quoteIdentifier(name),
      })), validFor: /^[\p{L}\p{N}_$]*$/u };
    }
    const inspection: Inspection | null = await session.inspect(scope.statement.query, scope.prefix);
    if (context.aborted) return null;
    if (!inspection) return base;
    const qualifier = /([\p{L}_][\p{L}\p{N}_$]*)\.$/u.exec(scope.before.slice(0, -word.text.length))?.[1];
    const options = qualifier ? [] : [...(base?.options ?? [])];
    const counts = new Map<string, number>();
    for (const column of inspection.columns) counts.set(column.name, (counts.get(column.name) ?? 0) + 1);
    for (const column of inspection.columns) {
      if (qualifier && column.qualifier !== qualifier) continue;
      const ambiguous = !qualifier && counts.get(column.name)! > 1;
      const qualified = ambiguous && column.qualifier;
      options.push({ label: qualified ? `${column.qualifier}.${column.name}` : column.name, type: "property",
        detail: column.data_type, info: column.qualifier ? `Column from ${column.qualifier}` : "Bound pipeline column",
        apply: qualified ? `${quoteIdentifier(column.qualifier!)}.${quoteIdentifier(column.name)}` : quoteIdentifier(column.name) });
    }
    if (!qualifier) for (const fn of inspection.functions) options.push(snippetCompletion(
      `${quoteIdentifier(fn.name)}(\${})`, { label: fn.name, type: "function", detail: fn.signature },
    ));
    return { from: word.from, options, validFor: /^[\p{L}\p{N}_$]*$/u };
  };
}
