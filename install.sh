#!/usr/bin/env bash
set -euo pipefail

REPO="${AGNI_REPO:-malayh/tantra}"
HOME_DIR="${AGNI_HOME:-$HOME/.agni}"
BIN_DIR="$HOME_DIR/bin"

case "$(uname -s)" in
    Linux) os="linux" ;;
    Darwin) os="darwin" ;;
    *) echo "agni: unsupported operating system $(uname -s)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
    x86_64 | amd64) arch="x86_64" ;;
    arm64 | aarch64) arch="arm64" ;;
    *) echo "agni: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

asset="agni-$os-$arch"
case "$asset" in
    agni-linux-x86_64 | agni-darwin-arm64) ;;
    *) echo "agni: no release binary is published for $asset" >&2; exit 1 ;;
esac

url="https://github.com/$REPO/releases/latest/download/$asset"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "agni: downloading $asset"
curl -fsSL "$url" -o "$tmp"
chmod +x "$tmp"
mkdir -p "$BIN_DIR"
mv "$tmp" "$BIN_DIR/agni"
trap - EXIT

echo "agni: installed to $BIN_DIR/agni"
case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) echo "agni: add it to your PATH — export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac
echo "agni: set OPENAI_API_KEY and AGNI_MODELS before the first run"
