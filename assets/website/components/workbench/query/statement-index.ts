import type { Text, Transaction } from "@codemirror/state";
import { scanStatementSegments, type StatementSegment } from "./statements";

interface IndexedSegment extends StatementSegment { id: number }
export interface StatementIndex { segments: IndexedSegment[]; nextId: number; scannedCharacters: number }

export function createStatementIndex(doc: Text): StatementIndex {
  const segments = scanStatementSegments(doc.toString()).map((segment, id) => ({ ...segment, id }));
  return { segments, nextId: segments.length, scannedCharacters: doc.length };
}

/** Rescan the touched lexical segment. A lost delimiter invalidates the remaining suffix. */
export function updateStatementIndex(previous: StatementIndex, transaction: Transaction): StatementIndex {
  if (!transaction.docChanged) return previous;
  let first = transaction.startState.doc.length, last = 0;
  transaction.changes.iterChangedRanges((from, to) => { first = Math.min(first, from); last = Math.max(last, to); });
  const old = previous.segments;
  const startIndex = Math.max(0, old.findIndex(segment => segment.to >= first));
  let endIndex = old.findIndex(segment => segment.to >= last);
  if (endIndex < startIndex) endIndex = old.length - 1;
  const start = transaction.changes.mapPos(old[startIndex]?.from ?? 0, -1);
  let end = transaction.changes.mapPos(old[endIndex]?.to ?? transaction.startState.doc.length, 1);
  let scannedCharacters = end - start;
  let parsed = scanStatementSegments(transaction.state.doc.sliceString(start, end), start);
  if (end < transaction.state.doc.length && !parsed.at(-1)?.delimited) {
    end = transaction.state.doc.length; endIndex = old.length - 1;
    scannedCharacters += end - start;
    parsed = scanStatementSegments(transaction.state.doc.sliceString(start, end), start);
  }
  const reusable = new Map(old.slice(startIndex, endIndex + 1)
    .map(segment => [transaction.changes.mapPos(segment.from, -1), segment.id]));
  let nextId = previous.nextId;
  const replacement = parsed.map(segment => ({ ...segment, id: reusable.get(segment.from) ?? nextId++ }));
  const suffix = old.slice(endIndex + 1).map(segment => ({ ...segment,
    from: transaction.changes.mapPos(segment.from, 1), to: transaction.changes.mapPos(segment.to, 1),
    statement: segment.statement && { ...segment.statement,
      from: transaction.changes.mapPos(segment.statement.from, 1), to: transaction.changes.mapPos(segment.statement.to, 1) },
  }));
  return { segments: [...old.slice(0, startIndex), ...replacement, ...suffix], nextId,
    scannedCharacters };
}
