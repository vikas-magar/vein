#!/usr/bin/env python3
"""Run paired TPC-H queries in Vein and DuckDB and compare their rows.

Run from any directory with:
    VEIN_DATA_DIR=/path/to/tpch python3 assets/sql/tpch/compare.py

Pipe and ANSI query text lives under assets/sql/tpch/{pipe,ansi}. Query IDs
pair the files, and the Pipe file declares output columns for empty results.
The displayed timings include process startup and are not a statistical
performance benchmark.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
PIPE_QUERY_FILE = ROOT / "assets" / "sql" / "tpch" / "pipe" / "queries.pipe"
ANSI_QUERY_FILE = ROOT / "assets" / "sql" / "tpch" / "ansi" / "queries.sql"
QUERY_MARKER = re.compile(r"(?m)^[ \t]*--[ \t]*name:[ \t]*([a-zA-Z0-9_-]+)[ \t]*$")
COLUMNS_MARKER = re.compile(r"(?m)^[ \t]*--[ \t]*columns:[ \t]*(.*?)[ \t]*$")
DEFAULT_MAX_ROWS = 100_000


class ComparisonError(Exception):
    pass


def run(command: list[str], *, cwd: Path, env: dict[str, str]) -> tuple[subprocess.CompletedProcess[str], float]:
    started = time.perf_counter()
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    return result, time.perf_counter() - started


def display_vein_rows(
    output: str, expected_columns: tuple[str, ...]
) -> tuple[tuple[str, ...], list[tuple[str, ...]]]:
    rows: list[tuple[str, ...]] = []
    header: tuple[str, ...] | None = None

    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        cells = tuple(
            (cell[1:] if cell.startswith(" ") else cell).rstrip()
            for cell in line[1:-1].split("|")
        )
        if all(re.fullmatch(r"[-+:\s]+", cell) for cell in cells):
            continue
        if header is None:
            header = cells
            if tuple(name.casefold() for name in header) != tuple(
                name.casefold() for name in expected_columns
            ):
                raise ComparisonError(
                    f"Vein result columns {header!r} differ from declared columns "
                    f"{expected_columns!r}"
                )
            continue
        if len(cells) == len(header):
            rows.append(cells)

    if header is None:
        if "Query returned 0 rows" in output:
            return expected_columns, []
        raise ComparisonError("could not read the Vein CLI result table")

    for line in output.splitlines():
        if " rows shown)" in line:
            raise ComparisonError(
                "Vein truncated the result table; rerun with a larger --max-rows value"
            )
    return header, rows


def duckdb_comparison_sql(sql: str, columns: tuple[str, ...], data_root: Path) -> str:
    escaped_root = str(data_root).replace("'", "''")
    query = sql.replace("{{DATA_ROOT}}", escaped_root).strip().rstrip(";")
    if "{{DATA_ROOT}}" in query:
        raise ComparisonError("ANSI query contains an unresolved {{DATA_ROOT}} placeholder")
    projection = ", ".join(
        f'CAST(comparison_result."{column.replace(chr(34), chr(34) * 2)}" AS VARCHAR) '
        f'AS "vein_col_{index}"'
        for index, column in enumerate(columns)
    )
    return f"SELECT {projection} FROM ({query}) AS comparison_result"


def duckdb_rows(output: str, column_count: int) -> list[tuple[str, ...]]:
    try:
        result: Any = json.loads(output)
    except json.JSONDecodeError as error:
        raise ComparisonError(f"could not parse DuckDB JSON output: {error}") from error
    if not isinstance(result, list):
        raise ComparisonError("DuckDB output was not a JSON row array")

    rows: list[tuple[str, ...]] = []
    for item in result:
        if not isinstance(item, dict):
            raise ComparisonError("DuckDB returned a row with an unexpected shape")
        row = []
        for index in range(column_count):
            value = item.get(f"vein_col_{index}")
            # The Vein CLI uses Arrow's pretty table, which drops trailing cell
            # whitespace. Match that display boundary when comparing DuckDB.
            row.append("NULL" if value is None else str(value).rstrip())
        rows.append(tuple(row))
    return rows


def show_error(engine: str, result: subprocess.CompletedProcess[str]) -> None:
    message = result.stderr.strip() or result.stdout.strip() or "process failed without output"
    lines = message.splitlines()
    print(f"  {engine} error:")
    print("\n".join(f"    {line}" for line in lines[-20:]))


def load_queries(selected: set[str]) -> list[dict[str, Any]]:
    pipe_queries = parse_query_file(PIPE_QUERY_FILE, with_columns=True)
    ansi_queries = parse_query_file(ANSI_QUERY_FILE, with_columns=False)
    if list(pipe_queries) != list(ansi_queries):
        raise ComparisonError(
            "Pipe and ANSI query IDs or order differ: "
            f"Pipe={list(pipe_queries)}, ANSI={list(ansi_queries)}"
        )
    queries = [
        {
            "id": query_id,
            "pipe": pipe_queries[query_id]["sql"],
            "columns": pipe_queries[query_id]["columns"],
            "ansi": ansi_queries[query_id]["sql"],
        }
        for query_id in pipe_queries
    ]
    if selected:
        queries = [query for query in queries if query["id"] in selected]
        found = {query["id"] for query in queries}
        missing = selected - found
        if missing:
            raise ComparisonError(f"unknown query id(s): {', '.join(sorted(missing))}")
    return queries


def parse_query_file(path: Path, *, with_columns: bool) -> dict[str, dict[str, Any]]:
    try:
        source = path.read_text(encoding="utf-8")
    except OSError as error:
        raise ComparisonError(f"cannot read query file {path}: {error}") from error

    markers = list(QUERY_MARKER.finditer(source))
    if not markers:
        raise ComparisonError(f"query file has no '-- name:' blocks: {path}")

    queries: dict[str, dict[str, Any]] = {}
    for index, marker in enumerate(markers):
        query_id = marker.group(1)
        if query_id in queries:
            raise ComparisonError(f"duplicate query ID {query_id!r} in {path}")
        end = markers[index + 1].start() if index + 1 < len(markers) else len(source)
        block = source[marker.end() : end]
        columns: tuple[str, ...] = ()
        if with_columns:
            column_marker = COLUMNS_MARKER.search(block)
            if column_marker is None:
                raise ComparisonError(f"query {query_id!r} has no '-- columns:' declaration")
            columns = tuple(name.strip() for name in column_marker.group(1).split(","))
            if not columns or any(not column for column in columns):
                raise ComparisonError(f"query {query_id!r} has an invalid columns declaration")
            block = COLUMNS_MARKER.sub("", block, count=1)
        sql = block.strip()
        if not sql:
            raise ComparisonError(f"query {query_id!r} has an empty SQL body in {path}")
        queries[query_id] = {"sql": sql, "columns": columns}
    return queries


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query_ids", nargs="*", help="optional IDs from the Pipe/ANSI query files")
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="TPC-H table root; defaults to VEIN_DATA_DIR or ./data/tpch",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=DEFAULT_MAX_ROWS,
        help="maximum Vein result rows to render (0 renders all rows)",
    )
    args = parser.parse_args()
    if args.max_rows < 0:
        parser.error("--max-rows must be zero or greater")

    data_root = args.data_dir or Path(
        os.environ.get("VEIN_DATA_DIR", ROOT / "data" / "tpch")
    )
    data_root = data_root.expanduser().resolve()
    duckdb_binary = os.environ.get("DUCKDB_BIN") or shutil.which("duckdb")
    if not duckdb_binary:
        print("DuckDB CLI was not found; install it or set DUCKDB_BIN", file=sys.stderr)
        return 2

    try:
        queries = load_queries(set(args.query_ids))
    except ComparisonError as error:
        print(error, file=sys.stderr)
        return 2
    if not queries:
        print("No comparison queries selected", file=sys.stderr)
        return 2

    base_env = os.environ.copy()
    base_env["VEIN_DATA_DIR"] = str(data_root)
    base_env["VEIN_DISPLAY_ROWS"] = str(args.max_rows)

    version, _ = run([duckdb_binary, "--version"], cwd=ROOT, env=base_env)
    if version.returncode != 0:
        show_error("DuckDB version check", version)
        return 2

    print(f"DuckDB: {version.stdout.strip()}")
    print(f"TPC-H data: {data_root}")
    print("Row comparison ignores order and preserves duplicate rows.")
    print("Trailing whitespace is normalized because Arrow's display table hides it.")

    failures = 0
    for query in queries:
        query_id = query["id"]
        columns = query["columns"]
        vein_command = ["cargo", "run", "--quiet", "-p", "cli", "--", query["pipe"]]
        vein_result, vein_seconds = run(vein_command, cwd=ROOT, env=base_env)

        sql = duckdb_comparison_sql(query["ansi"], columns, data_root)
        duckdb_command = [duckdb_binary, "-json", ":memory:", "-c", sql]
        duckdb_result, duckdb_seconds = run(duckdb_command, cwd=ROOT, env=base_env)

        if vein_result.returncode != 0 or duckdb_result.returncode != 0:
            failures += 1
            print(f"\n[ERROR] {query_id}")
            if vein_result.returncode != 0:
                show_error("Vein", vein_result)
            if duckdb_result.returncode != 0:
                show_error("DuckDB", duckdb_result)
            continue

        try:
            _, left = display_vein_rows(vein_result.stdout, columns)
            right = duckdb_rows(duckdb_result.stdout, len(columns))
        except ComparisonError as error:
            failures += 1
            print(f"\n[ERROR] {query_id}: {error}")
            continue

        left_counts = Counter(left)
        right_counts = Counter(right)
        if left_counts == right_counts:
            print(
                f"\n[PASS] {query_id}: {len(left)} rows match "
                f"(Vein {vein_seconds:.3f}s, DuckDB {duckdb_seconds:.3f}s)"
            )
        else:
            failures += 1
            print(f"\n[DIFF] {query_id}: Vein {len(left)} rows, DuckDB {len(right)} rows")
            print(f"  Vein-only rows: {list((left_counts - right_counts).items())[:3]}")
            print(f"  DuckDB-only rows: {list((right_counts - left_counts).items())[:3]}")

    print(f"\nSummary: {len(queries) - failures}/{len(queries)} queries matched")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
