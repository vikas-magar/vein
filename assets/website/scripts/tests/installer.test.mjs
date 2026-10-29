import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, mkdir, writeFile, readFile, rm, stat } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const execute = promisify(execFile);
const installer = fileURLToPath(new URL("../../public/install.sh", import.meta.url));

async function fixture(platform = "Linux", architecture = "x86_64") {
  const root = await mkdtemp(join(tmpdir(), "vein-installer-test-"));
  for (const name of ["bin", "assets", "source"]) await mkdir(`${root}/${name}`);
  await writeFile(`${root}/source/vein`, "#!/bin/sh\nprintf 'fixture CLI\\n'\n");
  const asset = platform === "Darwin" ? "vein-macos-aarch64.tar.gz" : "vein-linux-x86_64.tar.gz";
  await execute("tar", ["-C", `${root}/source`, "-czf", `${root}/assets/${asset}`, "vein"]);
  const checksum = createHash("sha256").update(await readFile(`${root}/assets/${asset}`)).digest("hex");
  await writeFile(`${root}/assets/${asset}.sha256`, `${checksum}  ${asset}\n`);
  await writeFile(`${root}/bin/uname`, `#!/bin/sh\ncase "$1" in -s) echo '${platform}';; -m) echo '${architecture}';; esac\n`, { mode: 0o755 });
  // The installer is exercised offline; these URLs never reach the network.
  await writeFile(`${root}/bin/curl`, `#!/bin/sh
set -eu
url=''
out=''
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift 2 ;;
    http*) url=$1; shift ;;
    *) shift ;;
  esac
done
cp "$FIXTURE_ASSETS/\${url##*/}" "$out"
`, { mode: 0o755 });
  const env = { ...process.env, PATH: `${root}/bin:${process.env.PATH}`,
    FIXTURE_ASSETS: `${root}/assets`, VEIN_INSTALL_DIR: `${root}/installed` };
  return { root, env, asset };
}

test("installer verifies and installs matching Linux and macOS assets without root", async () => {
  for (const [platform, architecture] of [["Linux", "x86_64"], ["Darwin", "arm64"]]) {
    const { root, env } = await fixture(platform, architecture);
    try {
      await execute("sh", [installer], { env });
      assert.equal(await readFile(`${root}/installed/vein`, "utf8"), "#!/bin/sh\nprintf 'fixture CLI\\n'\n");
      assert.equal((await stat(`${root}/installed/vein`)).mode & 0o777, 0o755);
    } finally { await rm(root, { recursive: true, force: true }); }
  }
});

test("checksum mismatch never replaces an existing installation", async () => {
  const { root, env, asset } = await fixture();
  try {
    await mkdir(`${root}/installed`); await writeFile(`${root}/installed/vein`, "existing");
    await writeFile(`${root}/assets/${asset}.sha256`, `${"0".repeat(64)}  ${asset}\n`);
    await assert.rejects(execute("sh", [installer], { env }), /Checksum mismatch/);
    assert.equal(await readFile(`${root}/installed/vein`, "utf8"), "existing");
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("unsupported architectures fail with an actionable message", async () => {
  const { root, env } = await fixture("Darwin", "x86_64");
  try { await assert.rejects(execute("sh", [installer], { env }), /Supported release targets/); }
  finally { await rm(root, { recursive: true, force: true }); }
});
