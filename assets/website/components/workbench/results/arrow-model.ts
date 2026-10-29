import type { Table } from "apache-arrow";
import { formatCell } from "./format.mjs";

/** The grid holds column references, never a converted array of result rows. */
export class ArrowGridModel {
  readonly fields;
  private columns;
  private formatted = new Map<string, string>();
  constructor(readonly table: Table, private cacheLimit = 512) {
    this.fields = table.schema.fields;
    this.columns = this.fields.map((_, index) => table.getChildAt(index));
  }
  get rows() { return this.table.numRows; }
  get columnCount() { return this.fields.length; }
  text(row: number, column: number): string {
    const key = `${row}:${column}`;
    const cached = this.formatted.get(key);
    if (cached !== undefined) return cached;
    const text = formatCell(this.columns[column]?.get(row), this.fields[column].type);
    if (this.formatted.size >= this.cacheLimit) this.formatted.delete(this.formatted.keys().next().value!);
    this.formatted.set(key, text);
    return text;
  }
  isNull(row: number, column: number) { return this.columns[column]?.get(row) == null; }
}
