from __future__ import annotations

import io
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
    find_column,
    is_overseas_etf,
    manager_from_text,
    normalize_columns,
    parse_float,
)


def session_with_retries() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
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


def load_etf_universe(session: requests.Session) -> pd.DataFrame:
    raw = fetch_bytes(session, FUNETF_FILTER_EXCEL_URL)
    xls = pd.read_excel(io.BytesIO(raw))
    xls.columns = normalize_columns(xls.columns)

    fund_code_col = find_column(xls, ["펀드코드", "fund code"], default_idx=0)
    name_col = find_column(xls, ["etf 종목명", "종목명", "상품명"], default_idx=2)
    short_col = find_column(xls, ["etf 단축코드", "단축코드", "종목코드"], default_idx=3)
    replica_col = find_column(xls, ["복제방식"], default_idx=28)
    aum_col = find_column(xls, ["운용규모(억원)", "운용규모"], default_idx=22)
    etf_type_col = find_column(xls, ["etf 대유형"], default_idx=3)
    etf_subtype_col = find_column(xls, ["etf 소유형"], default_idx=4)
    representative_main_col = find_column(xls, ["etf 대표유형 대유형"], default_idx=31)
    representative_sub_col = find_column(xls, ["etf 대표유형 소유형"], default_idx=32)
    benchmark_col = find_column(xls, ["기초지수"], default_idx=27)

    manager_col = None
    for candidate in ["운용사명", "운용사", "자산운용사", "issuer"]:
        try:
            manager_col = find_column(xls, [candidate])
            break
        except KeyError:
            continue

    records: list[dict] = []
    for _, row in xls.iterrows():
        etf_name = str(row[name_col]).strip()
        if not etf_name or etf_name == "nan":
            continue

        replica_type = str(row[replica_col]).strip()
        if "액티브" not in replica_type:
            continue

        row_manager = manager_from_text(row[manager_col]) if manager_col else None
        name_manager = manager_from_text(etf_name)
        manager = row_manager or name_manager
        if manager not in MANAGER_RULES:
            continue

        fund_code = str(row[fund_code_col]).strip()
        short_code = str(row[short_col]).strip()
        if not fund_code:
            continue

        asset_class = classify_asset_class(
            str(row[etf_type_col]),
            str(row[etf_subtype_col]),
            str(row[representative_main_col]),
            str(row[representative_sub_col]),
            etf_name,
        )
        style = classify_style(replica_type, str(row[etf_type_col]), etf_name)
        benchmark_name = str(row[benchmark_col])
        representative_sub = str(row[representative_sub_col])
        if is_overseas_etf(
            str(row[etf_type_col]),
            str(row[etf_subtype_col]),
            str(row[representative_main_col]),
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
