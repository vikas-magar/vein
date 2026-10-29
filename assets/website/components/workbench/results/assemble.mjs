import { Table } from "apache-arrow";

/** Assemble batch metadata once, preserving the transferred Arrow column buffers. */
export function assembleArrowTables(tables) {
  if (tables.length === 1) return tables[0];
  return new Table(tables[0].schema, tables.flatMap(table => table.batches));
}
