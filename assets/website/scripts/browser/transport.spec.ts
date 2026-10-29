import { expect, test } from "@playwright/test";
import { readFile, rm } from "node:fs/promises";
import { tableFromIPC } from "apache-arrow";
import { addParquetTable, createDataDirectory, replaceQuery } from "./fixtures";

let directory: string;
test.beforeAll(async () => {
  directory = await createDataDirectory("SELECT i::BIGINT AS id, repeat(i::VARCHAR, 64) AS payload FROM range(1, 10001) t(i)");
});
test.afterAll(async () => {
  if (directory) await rm(directory, { recursive: true, force: true });
});

test("records real result transport sizes and timings without converting rows", async ({ page }) => {
  await page.addInitScript(() => {
    const Original = window.Worker;
    const packets: number[] = [];
    Object.assign(window, { transportPackets: packets });
    window.Worker = class extends Original {
      constructor(url: string | URL, options?: WorkerOptions) {
        super(url, options);
        this.addEventListener("message", ({ data }) => {
          if (data.type === "result-batch" && data.buffer instanceof ArrayBuffer) {
            packets.push(data.buffer.byteLength);
          }
        });
      }
    };
  });
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await replaceQuery(page, "FROM smoke |> SELECT id, payload");
  for (let attempt = 0; attempt < 3; attempt++) {
    await page.evaluate(() => { (window as unknown as { transportPackets: number[] }).transportPackets.length = 0; });
    await page.getByRole("button", { name: "Run", exact: true }).click();
    const results = page.getByRole("region", { name: "Query results", exact: true });
    await expect(results).toHaveAttribute("data-state", "completed", { timeout: 30_000 });
    await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", "10001");
    const packets = await page.evaluate(() => (window as unknown as { transportPackets: number[] }).transportPackets);
    expect(packets.length).toBeGreaterThan(1);
    expect(Math.max(...packets)).toBeLessThanOrEqual(1024 * 1024);
    console.log(JSON.stringify({ browser: test.info().project.name, attempt, packets: packets.length,
      maxPacketBytes: Math.max(...packets), totalPacketBytes: packets.reduce((a, b) => a + b, 0),
      summary: await page.getByRole("group", { name: "Result summary", exact: true }).innerText() }));
    if (attempt === 0) {
      const pending = page.waitForEvent("download");
      await page.getByRole("button", { name: "Arrow", exact: true }).click();
      const downloaded = tableFromIPC(await readFile((await (await pending).path())!));
      expect(downloaded.numRows).toBe(10000);
      expect(String(downloaded.getChild("id")!.get(9999))).toBe("10000");
      expect(downloaded.getChild("payload")!.get(0)).toBe("1".repeat(64));
      expect(downloaded.getChild("payload")!.get(9999)).toBe("10000".repeat(64));
    }
  }
});

test("stopping while an IPC acknowledgement is withheld discards partial results", async ({ page }) => {
  await page.addInitScript(() => {
    const post = Worker.prototype.postMessage;
    let held = false;
    Object.assign(window, { heldAcknowledgement: false });
    Worker.prototype.postMessage = function (message, ...options: [transfer?: Transferable[] | StructuredSerializeOptions]) {
      if (message.type === "result-ack" && !held) {
        held = true; Object.assign(window, { heldAcknowledgement: true }); return;
      }
      return Reflect.apply(post, this, [message, ...options]);
    };
  });
  await page.goto("/");
  await page.locator('input[type="file"]').setInputFiles(directory);
  await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
  await replaceQuery(page, "FROM smoke |> SELECT id, payload");
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect.poll(() => page.evaluate(() => (window as unknown as { heldAcknowledgement: boolean }).heldAcknowledgement)).toBe(true);
  await expect(page.getByRole("grid")).toHaveCount(0);
  await page.getByRole("button", { name: "Stop execution", exact: true }).click();
  await expect(page.getByRole("region", { name: "Query results", exact: true })).toHaveAttribute("data-state", "stopped");
  await expect(page.getByRole("grid")).toHaveCount(0);
  await page.getByRole("button", { name: "Run", exact: true }).click();
  await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", "10001", { timeout: 30_000 });
});

test("streams Q10-shaped results larger than two output batches", async ({ page }) => {
  const rows = 43_838;
  const q10Directory = await createDataDirectory(
    `SELECT i::BIGINT AS c_custkey,
            'Customer#' || i::VARCHAR AS c_name,
            i::DECIMAL(15, 2) AS c_acctbal,
            'Phone#' || i::VARCHAR AS c_phone,
            1::BIGINT AS c_nationkey,
            'Address#' || i::VARCHAR AS c_address,
            'Comment#' || i::VARCHAR AS c_comment
     FROM range(1, ${rows + 1}) t(i)`,
    "customer",
  );
  try {
    await addParquetTable(
      q10Directory,
      "orders",
      `SELECT i::BIGINT AS o_orderkey, i::BIGINT AS o_custkey,
              DATE '1994-10-15' AS o_orderdate
       FROM range(1, ${rows + 1}) t(i)`,
    );
    await addParquetTable(
      q10Directory,
      "lineitem",
      `SELECT i::BIGINT AS l_orderkey, 'R'::VARCHAR AS l_returnflag,
              i::DECIMAL(15, 2) AS l_extendedprice, 0::DECIMAL(15, 2) AS l_discount
       FROM range(1, ${rows + 1}) t(i)`,
    );
    await addParquetTable(q10Directory, "nation", "SELECT 1::BIGINT AS n_nationkey, 'NATION'::VARCHAR AS n_name");

    await page.goto("/");
    await page.locator('input[type="file"]').setInputFiles(q10Directory);
    await expect(page.getByRole("status")).toContainText("Ready", { timeout: 30_000 });
    await replaceQuery(page, `FROM customer
|> JOIN orders ON c_custkey = o_custkey
|> JOIN lineitem ON o_orderkey = l_orderkey
|> JOIN nation ON c_nationkey = n_nationkey
|> WHERE o_orderdate >= DATE '1994-10-01' AND o_orderdate < DATE '1994-12-01'
   AND l_returnflag = 'R'
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue
   GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
|> ORDER revenue DESC`);
    const results = page.getByRole("region", { name: "Query results", exact: true });
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await expect(results).toHaveAttribute("data-state", "completed", { timeout: 60_000 });
    await expect(page.getByRole("grid")).toHaveAttribute("aria-rowcount", String(rows + 1));
    const summary = await page.getByRole("group", { name: "Result summary", exact: true }).innerText();
    console.log(JSON.stringify({ browser: test.info().project.name, rows, summary }));

    const pending = page.waitForEvent("download");
    await page.getByRole("button", { name: "Arrow", exact: true }).click();
    const download = await pending;
    const downloadPath = await download.path();
    if (!downloadPath) throw new Error("Arrow export did not produce a download path");
    const downloaded = tableFromIPC(await readFile(downloadPath));
    expect(downloaded.numRows).toBe(rows);
    expect(String(downloaded.getChild("c_custkey")!.get(0))).toBe(String(rows));
    expect(String(downloaded.getChild("c_custkey")!.get(rows - 1))).toBe("1");
  } finally {
    await rm(q10Directory, { recursive: true, force: true });
  }
});
