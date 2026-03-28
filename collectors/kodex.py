from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

from config import REQUEST_TIMEOUT

KODEX_PRODUCT_SEARCH_URL = "https://www.samsungfund.com/api/v1/kodex/product.do"
KODEX_PRODUCT_URL = "https://www.samsungfund.com/api/v1/kodex/product/{fid}.do"
KODEX_PDF_URL = "https://www.samsungfund.com/api/v1/kodex/product-pdf/{fid}.do"
KODEX_DETAIL_URL = "https://www.samsungfund.com/etf/product/view.do?id={fid}"

KODEX_API_HEADERS = {
    "Referer": "https://www.samsungfund.com/etf/product/list.do",
    "Origin": "https://www.samsungfund.com",
    "Accept": "application/json, text/plain, */*",
    "X-Requested-With": "XMLHttpRequest",
}


def _normalize_name(value: str) -> str:
    return "".join(str(value).lower().split())


def search_kodex_fid(
    session: requests.Session,
    etf_name: str,
    short_code: str,
    catalog: list[dict] | None = None,
) -> tuple[str, str]:
    items = catalog if catalog is not None else load_kodex_catalog(session)
    if not isinstance(items, list):
        items = []

    normalized_name = _normalize_name(etf_name)
    for item in items:
        if str(item.get("stkTicker", "")).strip() == str(short_code).strip():
            fid = str(item.get("fId", "")).strip()
            if fid:
                return fid, str(item.get("fNm", "")).strip()

    for item in items:
        if _normalize_name(str(item.get("fNm", "")).strip()) == normalized_name:
            fid = str(item.get("fId", "")).strip()
            if fid:
                return fid, str(item.get("fNm", "")).strip()

    response = session.get(
        KODEX_PRODUCT_SEARCH_URL,
        params={
            "srchTerm": "w",
            "ordrSort": "DESC",
            "ordrColm": "NAV",
            "pageNo": 1,
            "srchVal": short_code,
        },
        headers=KODEX_API_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    items = response.json()
    if isinstance(items, list):
        for item in items:
            if str(item.get("stkTicker", "")).strip() == str(short_code).strip():
                fid = str(item.get("fId", "")).strip()
                if fid:
                    return fid, str(item.get("fNm", "")).strip()

    raise RuntimeError("KODEX fId not found from official product search")


def load_kodex_catalog(session: requests.Session) -> list[dict]:
    response = session.get(
        KODEX_PRODUCT_SEARCH_URL,
        params={
            "srchTerm": "w",
            "ordrSort": "DESC",
            "ordrColm": "NAV",
            "pageNo": 1,
            "pageRows": 500,
        },
        headers=KODEX_API_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    items = response.json()
    if not isinstance(items, list) or not items:
        raise RuntimeError("KODEX catalog returned no rows")
    return items


def fetch_kodex_holdings(
    session: requests.Session,
    etf_name: str,
    manager: str,
    short_code: str,
    fund_code: str,
    catalog: list[dict] | None = None,
) -> tuple[str, str | None, pd.DataFrame]:
    fid, official_name = search_kodex_fid(session, etf_name, short_code, catalog=catalog)

    product_response = session.get(
        KODEX_PRODUCT_URL.format(fid=fid),
        headers=KODEX_API_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    product_response.raise_for_status()
    product_payload = product_response.json()
    gijun_ymd = str(product_payload.get("info", {}).get("product", {}).get("gijunYMD", "")).strip()
    if not gijun_ymd:
        gijun_ymd = str(product_payload.get("quickInfo", {}).get("gijunYMD", "")).strip()

    formatted_gijun = gijun_ymd
    asof_date = None
    if len(gijun_ymd) == 8 and gijun_ymd.isdigit():
        formatted_gijun = f"{gijun_ymd[:4]}.{gijun_ymd[4:6]}.{gijun_ymd[6:8]}"
        asof_date = datetime.strptime(gijun_ymd, "%Y%m%d").date().isoformat()

    pdf_response = session.get(
        KODEX_PDF_URL.format(fid=fid),
        params={"gijunYMD": formatted_gijun},
        headers=KODEX_API_HEADERS,
        timeout=REQUEST_TIMEOUT,
    )
    pdf_response.raise_for_status()
    pdf_payload = pdf_response.json()
    rows = pdf_payload.get("pdf", {}).get("list") or []
    if not rows:
        raise RuntimeError("KODEX product-pdf returned no rows")

    holdings_rows = []
    for item in rows:
        name = str(item.get("secNm", "")).strip()
        ratio = item.get("ratio")
        if not name or ratio in (None, "", "null"):
            continue
        holdings_rows.append(
            {
                "manager": manager,
                "etf_name": official_name or etf_name,
                "short_code": short_code,
                "fund_code": fund_code,
                "holding_name": name,
                "weight_pct": float(ratio),
                "asof_date": asof_date,
                "source": "KODEX",
            }
        )

    holdings_df = pd.DataFrame(holdings_rows)
    if holdings_df.empty:
        raise RuntimeError("KODEX holdings rows were empty")

    return KODEX_DETAIL_URL.format(fid=fid), asof_date, holdings_df
