export interface QueryStatement { from: number; to: number; query: string; number: number }
export interface StatementSegment {
  from: number; to: number; delimited: boolean;
  statement: Omit<QueryStatement, "number"> | null;
}

/** Find editor statement boundaries without splitting quoted text, comments or subqueries. */
export function queryStatements(text: string): QueryStatement[] {
  return scanStatementSegments(text).flatMap(segment => segment.statement ? [segment.statement] : [])
    .map((statement, index) => ({ ...statement, number: index + 1 }));
}

/** Neutral lexical boundaries permit safe incremental rescanning after an edit. */
export function scanStatementSegments(text: string, offset = 0): StatementSegment[] {
  const segments: StatementSegment[] = [];
  let segmentStart = 0;
  let start = 0, end = 0, depth = 0, block = 0;
  let quote = "", lineComment = false, hasCode = false;
  const finish = (to: number, delimited: boolean) => {
    segments.push({ from: offset + segmentStart, to: offset + to, delimited,
      statement: hasCode ? { from: offset + start, to: offset + end, query: text.slice(start, end).trim() } : null });
    segmentStart = to;
    hasCode = false;
  };
  for (let i = 0; i < text.length; i++) {
    const char = text[i], next = text[i + 1];
    if (lineComment) { if (char === "\n") lineComment = false; continue; }
    if (block) {
      if (char === "/" && next === "*") { block++; i++; }
      else if (char === "*" && next === "/") { block--; i++; }
      continue;
    }
    if (quote) {
      end = i + 1;
      if (char === quote) {
        if (next === quote) { end = ++i + 1; }
        else quote = "";
      }
      continue;
    }
    if (char === "-" && next === "-") { lineComment = true; i++; continue; }
    if (char === "/" && next === "*") { block = 1; i++; continue; }
    if (char === ";" && depth === 0) { finish(i + 1, true); start = i + 1; continue; }
    if (/\s/.test(char)) continue;
    if (!hasCode) { start = i; hasCode = true; }
    if (char === "'" || char === '"' || char === "`") quote = char;
    if (char === "(") depth++;
    if (char === ")") depth = Math.max(0, depth - 1);
    end = i + 1;
  }
  if (segmentStart < text.length) finish(text.length, false);
  return segments;
}

export function queryAt(text: string, position: number): QueryStatement | undefined {
  const statements = queryStatements(text);
  return statements.find(statement => position <= statement.to) ?? statements.at(-1);
}
