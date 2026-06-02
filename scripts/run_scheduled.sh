#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ".venv/bin/python" ]]; then
  python3 -m venv .venv
fi

".venv/bin/python" -m pip install -r requirements.txt
".venv/bin/python" main.py

git add data docs public
if git diff --staged --quiet; then
  echo "No generated output changes to commit."
  exit 0
fi

git commit -m "Refresh ETF data from local collector

The scheduled local collector replaces the previous GitHub Actions
workflow, so generated data must be committed from this machine for
the published static page to reflect the latest run.

Constraint: GitHub Actions collection is intentionally disabled
Constraint: macOS background jobs cannot read the Desktop workspace reliably
Rejected: Run the collector directly from Desktop via cron | macOS TCC blocked background access
Confidence: high
Scope-risk: narrow
Directive: Keep generated data, docs, and public outputs committed together
Tested: Scheduled collector generated data and static pages locally"
git push origin main
