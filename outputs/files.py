from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd

from config import DATA_DIR, DOCS_DIR, KODEX_LIST_URL, KST, PUBLIC_DIR, TIGER_ACTIVE_LINEUP_URL, TIME_LINEUP_URL
from config import FUNETF_FILTER_EXCEL_URL
from outputs.html_report import build_html


def build_run_summary(summary_df: pd.DataFrame, holdings_df: pd.DataFrame) -> dict:
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_date_kst": datetime.now(timezone.utc).astimezone(KST).isoformat(),
        "etf_count": int(len(summary_df.index)),
        "holding_count": int(len(holdings_df.index)),
    }


def write_outputs(summary_df: pd.DataFrame, holdings_df: pd.DataFrame) -> None:
    summary_df.to_csv(DATA_DIR / "etf_list.csv", index=False, encoding="utf-8-sig")
    holdings_df.to_csv(DATA_DIR / "etf_holdings.csv", index=False, encoding="utf-8-sig")

    (DATA_DIR / "etf_list.json").write_text(
        summary_df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8",
    )
    (DATA_DIR / "etf_holdings.json").write_text(
        holdings_df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8",
    )

    run_summary = build_run_summary(summary_df, holdings_df)
    (DATA_DIR / "run_summary.json").write_text(
        json.dumps(run_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    html = build_html(summary_df, holdings_df, run_summary=run_summary)
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")
    (PUBLIC_DIR / "index.html").write_text(html, encoding="utf-8")

    readme_note = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "etf_filter_excel": FUNETF_FILTER_EXCEL_URL,
            "time_lineup": TIME_LINEUP_URL,
            "tiger_active_lineup": TIGER_ACTIVE_LINEUP_URL,
            "kodex_list": KODEX_LIST_URL,
        },
        "note": "AUM과 구성종목은 공식 운용사 공개 페이지 기준이며, 장중 실시간 값과 차이날 수 있습니다.",
        "run_date_kst": datetime.now(timezone.utc).astimezone(KST).isoformat(),
    }
    (DATA_DIR / "meta.json").write_text(
        json.dumps(readme_note, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
