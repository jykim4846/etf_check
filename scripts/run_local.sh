#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  python3 -m venv .venv
fi

".venv/bin/python" -m pip install -r requirements.txt
".venv/bin/python" main.py

echo "Updated data/, docs/index.html, and public/index.html"
echo "Open docs/index.html directly or run: .venv/bin/python -m http.server 8000 -d docs"
