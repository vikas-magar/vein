# AGENTS.md — Vein

## Source control

- The existing `.aiignore` file is authoritative. Do not read, search, or open anything it matches:
  `.DS_Store`, `*.log`, `*.tmp`, `dist/`, `build/`, `out/`, `raw`, `docs`, `vein_data`, `data`, `README.md`.
- Do not create git commits or branches unless explicitly asked.
- Keep changes surgical: fix the root cause, match surrounding style, no drive-by refactors.

## Tooling contract: use RustRover MCP tools, not the shell

This workspace is driven through the JetBrains **RustRover MCP** server. Every repository
operation has a dedicated IDE tool. `execute_terminal_command` is a **last resort** — only for
something with no IDE equivalent.

### Task → tool

| Task | Tool |
| --- | --- |
| Read a file | `read_file` |
| Explore a directory | `list_directory_tree` |
| Find files (glob) | `search_file` |
| Find text / regex | `search_text`, `search_regex` |
| Find a symbol | `search_symbol`; then `get_symbol_info` for signature/docs |
| Call graph (callers / callees) | `analyze_calls` with `INCOMING_CALLS` / `OUTGOING_CALLS` |
| Jump the editor to a location | `open_file_in_editor` via `execute_tool "<file> --filePath <rel> --line <n>"` |
| Edit / create a file | `apply_patch`, `create_new_file` |
| Rename a symbol | `rename_refactoring` (never hand-edit references) |
| Format | `reformat_file` |
| Build / compile-check | `build_project` with `rebuild: false` (incremental); `filesToRebuild` for a single file |
| Discover run configs / tests | `get_run_configurations` (pass `filePath` to list run points) |
| Run a test or binary | `execute_run_configuration` (`configurationName`, or `filePath` + `line`) |
| Lint / inspections | `lint_files`, `get_file_problems` |
| Git status | `git_status` |
| Dependencies / modules | `get_project_dependencies`, `get_project_modules` |
| Changelists | `get_changelists`, `create_changelist`, `set_active_changelist`, `move_changes_to_changelist` |
| Debugging | `xdebug_*` family (breakpoints, session control, stack, frame values, evaluate, variables) |
| Database | `list_database_connections` → `list_database_schemas` → `execute_sql_query` / `preview_table_data` |
| Escape hatch for any other MCP tool | `execute_tool "<tool_name> --arg value"` |

### Rules

- Never shell out (`cat`, `ls`, `find`, `grep`, `cargo`, `git status`) when an IDE tool covers it.
- Validate every code change with `build_project` and run the relevant tests with
  `execute_run_configuration` — not `cargo` in a terminal.
- Always call `build_project` with `rebuild: false` (incremental). Never pass `rebuild: true`
  unless explicitly requested — a full rebuild is slow and is not part of the normal loop.
- Set the Air session title to the current task with `air_set_session_title`.
- `get_project_modules` currently reports `EMPTY_MODULE`; rely on `list_directory_tree` and
  `get_project_dependencies` for structure instead.

## Working agreement

- Per issue, the loop is: explain the issue → open the file and line in the editor → explain the
  fix and why → **wait for approval** → apply the patch → run the relevant tests → run the build.
- Cite locations as `path:line` so they are clickable.
- Explain design decisions (especially dependency or type changes) before making them.

## Project direction

- **Current focus: a robust, zero-copy single-node data engine.** Distributed execution is a
  later phase; do not build toward it yet.
- Zero copy is the core value proposition. Treat per-batch and per-row columnar copies as bugs,
  not optimizations. Metadata-level clones are lower priority but still worth removing.
