from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from collectors import (
    fetch_top10_holdings,
    load_etf_universe,
    resolve_item_id,
    session_with_retries,
)
from config import KST
from outputs import write_outputs


def build_holdings_from_row(row: pd.Series) -> pd.DataFrame:
    rows = []
    for idx in range(1, 6):
        name = str(row.get(f"top_{idx}", "")).strip()
        weight = row.get(f"top_{idx}_weight_pct")
        if not name or name == "nan" or weight is None or pd.isna(weight):
            continue
        rows.append(
            {
                "manager": row["manager"],
                "etf_name": row["etf_name"],
                "short_code": row["short_code"],
                "fund_code": row["fund_code"],
                "holding_name": name,
                "weight_pct": float(weight),
                "asof_date": row["asof_date"],
                "source": row["source"],
            }
        )
    return pd.DataFrame(rows)


def build_holdings_from_api(row: pd.Series, api_rows: list[dict]) -> pd.DataFrame:
    rows = []
    for item in api_rows:
        holding_name = str(item.get("citmNm") or item.get("grpItmNo") or "").strip()
        weight_pct = item.get("evP")
        if not holding_name or weight_pct is None:
            continue
        rows.append(
            {
                "manager": row["manager"],
                "etf_name": row["etf_name"],
                "short_code": row["short_code"],
                "fund_code": row["fund_code"],
                "holding_name": holding_name,
                "weight_pct": float(weight_pct),
                "asof_date": row["asof_date"],
                "source": row["source"],
            }
        )
    return pd.DataFrame(rows)


def collect_etf_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    session = session_with_retries()
    etf_df = load_etf_universe(session)
    if etf_df.empty:
        raise RuntimeError("액티브 ETF 목록을 찾지 못했습니다. 사이트 구조가 바뀌었는지 확인하세요.")

    holdings_frames: list[pd.DataFrame] = []
    enriched_rows: list[dict] = []

    for row in etf_df.to_dict(orient="records"):
        row_series = pd.Series(row)
        errors: list[str] = []
        item_id = None
        detail_url = ""

        try:
            item_id, detail_url = resolve_item_id(session, row["etf_name"], row["fund_code"])
        except Exception as exc:
            errors.append(f"resolve_item_id: {exc}")

        if not item_id:
            errors.append("item_id_not_found")

        row["detail_url"] = detail_url
        row_series["detail_url"] = detail_url

        holdings = pd.DataFrame()
        if item_id:
            try:
                api_rows = fetch_top10_holdings(session, item_id)
                holdings = build_holdings_from_api(row_series, api_rows)
                if holdings.empty:
                    errors.append("api_holdings_empty")
            except Exception as exc:
                errors.append(f"fetch_top10_holdings: {exc}")
                holdings = pd.DataFrame()

        if holdings.empty:
            holdings = build_holdings_from_row(row_series)
            if holdings.empty:
                errors.append("fallback_holdings_empty")

        holdings_frames.append(holdings)
        row["error"] = " ; ".join(dict.fromkeys(errors))
        enriched_rows.append(row)
        print(f"[OK] {row['etf_name']}")

    summary_df = pd.DataFrame(enriched_rows)
    summary_df["fetched_at_utc"] = datetime.now(timezone.utc).isoformat()
    summary_df["run_date_kst"] = datetime.now(timezone.utc).astimezone(KST).isoformat()
    if "error" not in summary_df.columns:
        summary_df["error"] = ""
    summary_df = summary_df.sort_values(
        by=["aum_okr", "manager", "etf_name"],
        ascending=[False, True, True],
        na_position="last",
    )
    summary_df = summary_df[
        [
            "manager",
            "etf_name",
            "short_code",
            "fund_code",
            "detail_url",
            "source",
            "asset_class",
            "style",
            "theme",
            "category_tags",
            "aum_okr",
            "aum_unit",
            "asof_date",
            "fetched_at_utc",
            "error",
            "run_date_kst",
        ]
    ]

    if holdings_frames:
        holdings_df = pd.concat(holdings_frames, ignore_index=True)
    else:
        holdings_df = pd.DataFrame(
            columns=[
                "manager",
                "etf_name",
                "short_code",
                "fund_code",
                "holding_name",
                "weight_pct",
                "asof_date",
                "source",
            ]
        )

    return summary_df, holdings_df


def run() -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_df, holdings_df = collect_etf_data()
    write_outputs(summary_df, holdings_df)
    return summary_df, holdings_df
