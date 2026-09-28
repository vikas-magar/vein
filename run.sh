#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if (($# == 0)); then
    printf 'Usage: %s "FROM table |> SELECT column"\n' "$0" >&2
    exit 2
fi

export VEIN_DATA_DIR="${VEIN_DATA_DIR:-./data}"
exec cargo run -p cli -- "$@"
