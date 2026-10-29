import { queryStatements } from "../query/statements";
import type { QueryDocument, ResultOrigin } from "../workspace/contracts";

/** Capture execution identity once, before edits, renames or delayed Worker replies. */
export function captureResultOrigin(document: QueryDocument, query: string, number?: number): ResultOrigin {
  const statements = queryStatements(document.text);
  const requested = number !== undefined ? statements[number - 1] : undefined;
  const matches = statements.filter(statement => statement.query === query);
  return { documentName: document.name, statementNumber: requested?.query === query ? requested.number
    : matches.length === 1 ? matches[0].number : null };
}
