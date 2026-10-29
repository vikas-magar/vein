import assert from "node:assert/strict";
import test from "node:test";
import { captureResultOrigin } from "../../components/workbench/results/origin.ts";
import { Documents } from "../../components/workbench/workspace/documents.ts";

test("execution identity captures the selected duplicate statement and survives edits and renames", () => {
  const document = { name: "report.vein", text: "FROM smoke;\nFROM smoke;" };
  const origin = captureResultOrigin(document, "FROM smoke", 2);
  document.name = "renamed.vein"; document.text = "FROM other";
  assert.deepEqual(origin, { documentName: "report.vein", statementNumber: 2 });
});

test("identity does not invent a statement number for ambiguous or external query text", () => {
  const document = { name: "report", text: "FROM smoke; FROM smoke; FROM other" };
  assert.equal(captureResultOrigin(document, "FROM smoke").statementNumber, null);
  assert.equal(captureResultOrigin(document, "FROM other").statementNumber, 3);
  assert.equal(captureResultOrigin(document, "FROM missing", 1).statementNumber, null);
  assert.equal(captureResultOrigin(document, "FROM other", 99).statementNumber, 3);
});

test("result identity follows its owning payload and eviction releases both", () => {
  const documents = new Documents(8);
  const first = documents.open("first", "FROM first"), second = documents.open("second", "FROM second");
  const origin = { documentName: "first", statementNumber: 1 };
  const table = { retained: true, numRows: 1 };
  documents.complete(first, { byteLength: 8, table, query: "FROM first" }, origin);
  assert.equal(documents.get(first).result.byteLength, 8);
  assert.equal(documents.get(first).result.table, table);
  assert.equal(documents.get(first).resultOrigin, origin);
  documents.complete(second, { byteLength: 8, table, query: "FROM second" });
  assert.equal(documents.get(first).result, null);
  assert.equal(documents.get(first).resultOrigin, null);
});
