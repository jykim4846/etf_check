from __future__ import annotations

import io
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .tiger import short_code_to_kr_isin
from config import (
    FUNETF_FILTER_EXCEL_URL,
    FUNETF_PDF_API_URL,
    FUNETF_SEARCH_API_URL,
    FUNETF_SEARCH_URL,
    HEADERS,
    KST,
    MANAGER_RULES,
    MAX_ETFS,
    REQUEST_TIMEOUT,
)
from transforms import (
    build_category_tags,
    classify_asset_class,
    classify_style,
    classify_theme,
    compact_text,
    find_column,
    find_column_optional,
    is_overseas_etf,
    manager_from_text,
    normalize_columns,
    parse_float,
)


def session_with_retries() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def fetch_bytes(session: requests.Session, url: str) -> bytes:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.content


def fetch_text(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def fetch_search_form(session: requests.Session, query: str) -> dict[str, str]:
    html = session.get(FUNETF_SEARCH_URL, params={"schVal": query}, timeout=REQUEST_TIMEOUT).text
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", {"name": "searchForm"})
    if form is None:
        raise RuntimeError("FunETF search form not found")
    return {
        inp.get("name"): inp.get("value", "")
        for inp in form.find_all("input")
        if inp.get("name")
    }


def resolve_item_id(session: requests.Session, query: str, fund_code: str) -> tuple[str | None, str]:
    form = fetch_search_form(session, query)
    params = {
        "schVal": form.get("schVal", query),
        "reSchVal": form.get("reSchVal", ""),
        "reSchChk": form.get("reSchChk", ""),
        "schMoreClass": form.get("schMoreClass", ""),
        "schKeyword": form.get("schKeyword", ""),
        "_csrf": form.get("_csrf", ""),
    }
    response = session.get(
        FUNETF_SEARCH_API_URL,
        params=params,
        headers={"X-Requested-With": "XMLHttpRequest"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    result = response.json()
    etf_list = result.get("etfList", {}).get("content", [])
    for item in etf_list:
        if item.get("fundCd") == fund_code:
            item_id = item.get("itemId")
            if item_id:
                return item_id, f"https://www.funetf.co.kr/product/etf/view/{item_id}"
    return None, ""


def fetch_top10_holdings(session: requests.Session, item_id: str) -> list[dict]:
    html = fetch_text(session, f"https://www.funetf.co.kr/product/etf/view/{item_id}")
    soup = BeautifulSoup(html, "html.parser")
    form = soup.find("form", {"name": "frm"})
    if form is None:
        return []

    params = {
        inp.get("name"): inp.get("value", "")
        for inp in form.find_all("input")
        if inp.get("name")
    }
    params["etfPdfYmd"] = (
        params.get("etfPdfYmd")
        or params.get("kodexPdfYmd")
        or params.get("gijunYmd")
        or ""
    )
    response = session.get(
        FUNETF_PDF_API_URL,
        params=params,
        headers={
            "Referer": f"https://www.funetf.co.kr/product/etf/view/{item_id}",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, list):
        return []
    return sorted(result, key=lambda item: float(item.get("evP") or 0), reverse=True)[:10]


def _row_text(row: pd.Series, column: str | None) -> str:
    if not column:
        return ""
    value = row.get(column, "")
    if pd.isna(value):
        return ""
    return str(value).strip()


def _supports_official_collection(manager: str, etf_name: str) -> bool:
    if manager == "타임폴리오":
        return True
    if manager == "미래에셋":
        return True
    if manager == "삼성" and str(etf_name).startswith("KODEX"):
        return True
    return False


def load_etf_universe(session: requests.Session) -> pd.DataFrame:
    raw = fetch_bytes(session, FUNETF_FILTER_EXCEL_URL)
    xls = pd.read_excel(io.BytesIO(raw))
    xls.columns = normalize_columns(xls.columns)

    fund_code_col = find_column_optional(xls, ["펀드코드", "fund code", "표준코드", "isin"])
    name_col = find_column(xls, ["etf 종목명", "종목명", "상품명"])
    short_col = find_column(xls, ["etf 단축코드", "단축코드", "종목코드"])
    replica_col = find_column_optional(xls, ["복제방식"])
    aum_col = find_column(xls, ["운용규모(억원)", "운용규모"])
    etf_type_col = find_column(xls, ["etf 대유형"])
    etf_subtype_col = find_column_optional(xls, ["etf 소유형"])
    representative_main_col = find_column_optional(xls, ["etf 대표유형 대유형"])
    representative_sub_col = find_column_optional(xls, ["etf 대표유형 소유형"])
    benchmark_col = find_column_optional(xls, ["기초지수"])

    manager_col = None
    for candidate in ["운용사명", "운용사", "자산운용사", "issuer"]:
        try:
            manager_col = find_column(xls, [candidate])
            break
        except KeyError:
            continue

    records: list[dict] = []
    for _, row in xls.iterrows():
        etf_name = _row_text(row, name_col)
        if not etf_name or etf_name == "nan":
            continue

        short_code = _row_text(row, short_col)
        if not short_code:
            continue

        etf_type = _row_text(row, etf_type_col)
        etf_subtype = _row_text(row, etf_subtype_col)
        representative_main = _row_text(row, representative_main_col)
        representative_sub = _row_text(row, representative_sub_col)
        benchmark_name = _row_text(row, benchmark_col)
        replica_type = _row_text(row, replica_col)
        active_marker = compact_text(replica_type, etf_type, etf_name)
        if "액티브" not in active_marker:
            continue

        row_manager = manager_from_text(_row_text(row, manager_col)) if manager_col else None
        name_manager = manager_from_text(etf_name)
        manager = row_manager or name_manager
        if manager not in MANAGER_RULES:
            continue
        if not _supports_official_collection(manager, etf_name):
            continue

        fund_code = _row_text(row, fund_code_col) or short_code_to_kr_isin(short_code)
        if not fund_code:
            continue

        asset_class = classify_asset_class(
            etf_type,
            etf_subtype,
            representative_main,
            representative_sub,
            etf_name,
        )
        style = classify_style(replica_type, etf_type, etf_name)
        if is_overseas_etf(
            etf_type,
            etf_subtype,
            representative_main,
            representative_sub,
            etf_name,
            benchmark_name,
        ):
            continue

        theme = classify_theme(etf_name, representative_sub, benchmark_name)
        category_tags = build_category_tags(asset_class, style, theme, representative_sub)

        records.append(
            {
                "manager": manager,
                "etf_name": etf_name,
                "short_code": short_code,
                "fund_code": fund_code,
                "detail_url": "",
                "source": "FunETF",
                "aum_okr": parse_float(str(row[aum_col])),
                "aum_unit": "억원",
                "asof_date": datetime.now(timezone.utc).astimezone(KST).date().isoformat(),
                "asset_class": asset_class,
                "style": style,
                "theme": theme,
                "category_tags": category_tags,
                "top_1": str(row.get("TOP 1", "")).strip(),
                "top_1_weight_pct": parse_float(str(row.get("비율(%)", ""))),
                "top_2": str(row.get("TOP 2", "")).strip(),
                "top_2_weight_pct": parse_float(str(row.get("비율(%).1", ""))),
                "top_3": str(row.get("TOP 3", "")).strip(),
                "top_3_weight_pct": parse_float(str(row.get("비율(%).2", ""))),
                "top_4": str(row.get("TOP 4", "")).strip(),
                "top_4_weight_pct": parse_float(str(row.get("비율(%).3", ""))),
                "top_5": str(row.get("TOP 5", "")).strip(),
                "top_5_weight_pct": parse_float(str(row.get("비율(%).4", ""))),
            }
        )

    df = pd.DataFrame(records).drop_duplicates(subset=["fund_code"])
    if MAX_ETFS > 0:
        df = df.head(MAX_ETFS).copy()
    return df
