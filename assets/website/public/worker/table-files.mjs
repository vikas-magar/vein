// File objects retain browser-owned payloads; only their paths are inspected here.
export function tableFiles(files) {
  const selected = Array.from(files ?? []).filter(file => file.name.toLowerCase().endsWith(".parquet"));
  if (!selected.length) throw new Error("No Parquet files were found in that data directory.");
  const root = selected[0].webkitRelativePath?.split("/")[0];
  return validateTableEntries(selected.map(file => {
    const parts = file.webkitRelativePath?.split("/") ?? [];
    if (!root || parts[0] !== root || parts.length < 3) {
      throw new Error("Choose the parent data directory containing your Parquet table folders.");
    }
    const path = parts.slice(1).join("/");
    return { path, file };
  }));
}

export function validateTableEntries(entries) {
  if (!Array.isArray(entries) || !entries.length) throw new Error("No Parquet files were found in that data directory.");
  const paths = new Set();
  for (const { path, file } of entries) {
    const parts = typeof path === "string" ? path.split("/") : [];
    if (!file || parts.length < 2 || path.includes("\\") || parts.some(part => !part || part === "." || part === "..")) {
      throw new Error("Invalid directory entry.");
    }
    if (paths.has(path)) throw new Error(`Duplicate Parquet path: ${path}`);
    paths.add(path);
  }
  return entries.sort((left, right) => left.path.localeCompare(right.path));
}
