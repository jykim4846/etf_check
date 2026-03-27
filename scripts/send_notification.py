from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests


def load_summary() -> dict:
    summary_path = os.getenv("RUN_SUMMARY_PATH", "").strip()
    if not summary_path:
      return {}
    path = Path(summary_path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_message(status: str) -> str:
    emoji = "✅" if status == "success" else "❌"
    repo = os.getenv("GITHUB_REPOSITORY", "unknown-repo")
    run_id = os.getenv("GITHUB_RUN_ID", "")
    server_url = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    actor = os.getenv("GITHUB_ACTOR", "unknown")
    attempts = os.getenv("ATTEMPTS_USED", "1")
    run_url = f"{server_url}/{repo}/actions/runs/{run_id}" if run_id else server_url
    summary = load_summary()

    lines = [
        f"{emoji} ETF daily update {status}\n"
        f"repo: {repo}\n"
        f"actor: {actor}\n"
        f"attempts: {attempts}\n"
        f"run: {run_url}"
    ]

    if summary:
        lines.extend(
            [
                f"etfs: {summary.get('etf_count', 0)}",
                f"holdings: {summary.get('holding_count', 0)}",
                f"error-etfs: {summary.get('error_etf_count', 0)}",
                f"error-count: {summary.get('error_count', 0)}",
            ]
        )
        top_errors = summary.get("top_errors") or []
        if top_errors:
            lines.append("top-errors:")
            for item in top_errors[:3]:
                lines.append(f"- {item.get('message', 'unknown')} ({item.get('count', 0)})")

    return "\n".join(lines)


def main() -> None:
    status = sys.argv[1] if len(sys.argv) > 1 else "success"
    webhook_url = os.getenv("NOTIFY_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("NOTIFY_WEBHOOK_URL is not set; skipping notification.")
        return

    message = build_message(status)
    payload = {"text": message, "content": message}
    response = requests.post(webhook_url, json=payload, timeout=15)
    response.raise_for_status()
    print(f"Notification sent: {status}")


if __name__ == "__main__":
    main()
