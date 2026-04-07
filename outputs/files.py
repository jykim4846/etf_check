from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import DATA_DIR, DOCS_DIR, HISTORY_DIR, KODEX_LIST_URL, KST, PUBLIC_DIR, TIGER_ACTIVE_LINEUP_URL, TIME_LINEUP_URL
from config import FUNETF_FILTER_EXCEL_URL
from outputs.html_report import build_html


def build_run_summary(
    summary_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    holding_changes_df: pd.DataFrame | None = None,
    previous_run_summary: dict | None = None,
) -> dict:
    holding_changes_df = holding_changes_df if holding_changes_df is not None else pd.DataFrame()
    changed_mask = holding_changes_df.get("change_state", pd.Series(dtype="object")).isin(
        ["신규", "증가", "감소", "제외"]
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_date_kst": datetime.now(timezone.utc).astimezone(KST).isoformat(),
        "etf_count": int(len(summary_df.index)),
        "holding_count": int(len(holdings_df.index)),
        "previous_run_date_kst": (previous_run_summary or {}).get("run_date_kst"),
        "changed_holding_count": int(changed_mask.sum()) if not holding_changes_df.empty else 0,
        "new_holding_count": int((holding_changes_df.get("change_state") == "신규").sum()) if not holding_changes_df.empty else 0,
        "removed_holding_count": int((holding_changes_df.get("change_state") == "제외").sum()) if not holding_changes_df.empty else 0,
        "increased_holding_count": int((holding_changes_df.get("change_state") == "증가").sum()) if not holding_changes_df.empty else 0,
        "decreased_holding_count": int((holding_changes_df.get("change_state") == "감소").sum()) if not holding_changes_df.empty else 0,
    }


def _read_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_json_if_exists(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _snapshot_previous_outputs(snapshot_key: str) -> None:
    snapshot_dir = HISTORY_DIR / snapshot_key
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for file_name in [
        "etf_list.csv",
        "etf_holdings.csv",
        "etf_list.json",
        "etf_holdings.json",
        "run_summary.json",
        "meta.json",
    ]:
        source_path = DATA_DIR / file_name
        if source_path.exists():
            (snapshot_dir / file_name).write_bytes(source_path.read_bytes())


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _normalize_weight_schema(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    if "weight_pct" not in normalized.columns and "current_weight_pct" in normalized.columns:
        normalized["weight_pct"] = normalized["current_weight_pct"]
    if "current_weight_pct" not in normalized.columns and "weight_pct" in normalized.columns:
        normalized["current_weight_pct"] = normalized["weight_pct"]
    if "previous_weight_pct" not in normalized.columns:
        normalized["previous_weight_pct"] = pd.NA
    if "weight_diff_pct" not in normalized.columns:
        normalized["weight_diff_pct"] = pd.NA
    if "change_state" not in normalized.columns:
        normalized["change_state"] = pd.NA
    return normalized


def build_holding_changes(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    current = _normalize_weight_schema(current_df)
    previous = _normalize_weight_schema(previous_df)

    if current.empty:
        empty_columns = [
            "manager",
            "etf_name",
            "short_code",
            "fund_code",
            "holding_name",
            "current_weight_pct",
            "previous_weight_pct",
            "weight_diff_pct",
            "change_state",
            "asof_date",
            "source",
        ]
        empty_df = pd.DataFrame(columns=empty_columns)
        return empty_df, empty_df

    current["weight_pct"] = _to_numeric(current["weight_pct"])
    if previous.empty:
        current["previous_weight_pct"] = pd.NA
        current["weight_diff_pct"] = pd.NA
        current["change_state"] = "첫수집"
        current = current[
            [
                "manager",
                "etf_name",
                "short_code",
                "fund_code",
                "holding_name",
                "weight_pct",
                "previous_weight_pct",
                "weight_diff_pct",
                "change_state",
                "asof_date",
                "source",
            ]
        ].rename(columns={"weight_pct": "current_weight_pct"})
        return current, current.copy()

    previous["weight_pct"] = _to_numeric(previous["weight_pct"])
    previous_lookup = previous[
        [
            "fund_code",
            "holding_name",
            "weight_pct",
        ]
    ].rename(columns={"weight_pct": "_previous_weight_pct"})

    enriched = current.merge(
        previous_lookup,
        on=["fund_code", "holding_name"],
        how="left",
    )
    enriched["previous_weight_pct"] = enriched["_previous_weight_pct"]
    enriched["weight_diff_pct"] = enriched["weight_pct"] - enriched["previous_weight_pct"]
    enriched["change_state"] = "유지"
    enriched.loc[enriched["previous_weight_pct"].isna(), "change_state"] = "신규"
    enriched.loc[
        enriched["previous_weight_pct"].notna() & (enriched["weight_diff_pct"] > 0.0001),
        "change_state",
    ] = "증가"
    enriched.loc[
        enriched["previous_weight_pct"].notna() & (enriched["weight_diff_pct"] < -0.0001),
        "change_state",
    ] = "감소"
    enriched = enriched[
        [
            "manager",
            "etf_name",
            "short_code",
            "fund_code",
            "holding_name",
            "weight_pct",
            "previous_weight_pct",
            "weight_diff_pct",
            "change_state",
            "asof_date",
            "source",
        ]
    ].rename(columns={"weight_pct": "current_weight_pct"})

    current_keys = current[["fund_code", "holding_name"]].drop_duplicates()
    removed = previous.merge(current_keys, on=["fund_code", "holding_name"], how="left", indicator=True)
    removed = removed[removed["_merge"] == "left_only"].copy()
    if not removed.empty:
        removed["current_weight_pct"] = 0.0
        removed["previous_weight_pct"] = removed["weight_pct"]
        removed["weight_diff_pct"] = -removed["weight_pct"]
        removed["change_state"] = "제외"
        removed = removed[
            [
                "manager",
                "etf_name",
                "short_code",
                "fund_code",
                "holding_name",
                "current_weight_pct",
                "previous_weight_pct",
                "weight_diff_pct",
                "change_state",
                "asof_date",
                "source",
            ]
        ]
    else:
        removed = pd.DataFrame(columns=enriched.columns)

    holding_changes = pd.concat([enriched, removed], ignore_index=True)
    return enriched, holding_changes


def write_outputs(summary_df: pd.DataFrame, holdings_df: pd.DataFrame) -> None:
    previous_summary_df = _read_csv_if_exists(DATA_DIR / "etf_list.csv")
    previous_holdings_df = _read_csv_if_exists(DATA_DIR / "etf_holdings.csv")
    previous_run_summary = _read_json_if_exists(DATA_DIR / "run_summary.json")
    if not previous_summary_df.empty or not previous_holdings_df.empty:
        snapshot_key = datetime.now(timezone.utc).astimezone(KST).strftime("%Y%m%d-%H%M%S")
        _snapshot_previous_outputs(snapshot_key)

    current_holdings_df, holding_changes_df = build_holding_changes(holdings_df, previous_holdings_df)

    summary_df.to_csv(DATA_DIR / "etf_list.csv", index=False, encoding="utf-8-sig")
    current_holdings_df.to_csv(DATA_DIR / "etf_holdings.csv", index=False, encoding="utf-8-sig")
    holding_changes_df.to_csv(DATA_DIR / "holding_changes.csv", index=False, encoding="utf-8-sig")

    (DATA_DIR / "etf_list.json").write_text(
        summary_df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8",
    )
    (DATA_DIR / "etf_holdings.json").write_text(
        current_holdings_df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8",
    )
    (DATA_DIR / "holding_changes.json").write_text(
        holding_changes_df.to_json(orient="records", force_ascii=False, indent=2),
        encoding="utf-8",
    )

    run_summary = build_run_summary(
        summary_df,
        current_holdings_df,
        holding_changes_df=holding_changes_df,
        previous_run_summary=previous_run_summary,
    )
    (DATA_DIR / "run_summary.json").write_text(
        json.dumps(run_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    html = build_html(
        summary_df,
        current_holdings_df,
        run_summary=run_summary,
        holding_changes_df=holding_changes_df,
    )
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
