#!/bin/sh
# Install the CLI asset published by Vein's release workflow, without root access.
set -eu

fail() { printf 'Vein installer: %s\n' "$*" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || fail 'curl is required.'
command -v tar >/dev/null 2>&1 || fail 'tar is required.'

case "$(uname -s):$(uname -m)" in
  Darwin:arm64|Darwin:aarch64) platform=macos-aarch64 ;;
  Linux:x86_64|Linux:amd64) platform=linux-x86_64 ;;
  *) fail 'Supported release targets: macOS ARM64 and Linux x86-64 (glibc).' ;;
esac

archive="vein-${platform}.tar.gz"
base='https://github.com/vikas-magar/vein/releases/latest/download'
destination="${VEIN_INSTALL_DIR:-${HOME}/.local/bin}"
work=$(mktemp -d "${TMPDIR:-/tmp}/vein-install.XXXXXX")
staged=''
cleanup() { rm -rf "$work"; if [ -n "$staged" ]; then rm -f "$staged"; fi; }
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

printf 'Downloading Vein for %s…\n' "$platform"
curl -fsSL "$base/$archive" -o "$work/$archive" || fail 'No downloadable release asset. Check that a public release has been published.'
curl -fsSL "$base/$archive.sha256" -o "$work/checksum" || fail 'Release checksum is unavailable.'
read -r expected remainder < "$work/checksum" || fail 'Invalid checksum file.'
case "$expected" in ''|*[!0123456789abcdefABCDEF]*) fail 'Invalid SHA-256 checksum.' ;; esac
[ "${#expected}" -eq 64 ] || fail 'Invalid SHA-256 checksum length.'
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$work/$archive")
elif command -v shasum >/dev/null 2>&1; then
  actual=$(shasum -a 256 "$work/$archive")
else
  fail 'sha256sum or shasum is required.'
fi
[ "${actual%% *}" = "$expected" ] || fail 'Checksum mismatch; nothing installed.'
[ "$(tar -tzf "$work/$archive")" = 'vein' ] || fail 'Unexpected archive contents.'
tar -xzf "$work/$archive" -C "$work"
[ -f "$work/vein" ] && [ ! -L "$work/vein" ] || fail 'Release does not contain a regular CLI executable.'
mkdir -p "$destination"
staged="$destination/.vein-install.$$"
cp "$work/vein" "$staged"
chmod 755 "$staged"
mv -f "$staged" "$destination/vein"
staged=''
printf 'Installed %s/vein\n' "$destination"
case ":${PATH}:" in
  *":${destination}:"*) printf 'Run: vein "FROM your_table |> AGGREGATE count(*) AS n"\n' ;;
  *) printf 'Add this directory to PATH: %s\n' "$destination" ;;
esac
