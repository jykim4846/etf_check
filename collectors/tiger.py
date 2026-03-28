from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import REQUEST_TIMEOUT

TIGER_PDF_URL = "https://investments.miraeasset.com/tigeretf/ko/product/search/detail/pdf.ajax"
TIGER_ITEMS_URL = "https://investments.miraeasset.com/tigeretf/ko/product/chart/prdct-item-list.ajax"
TIGER_DETAIL_URL = "https://investments.miraeasset.com/tigeretf/front/products/product.do?ksdFund={ksd_fund}"


def short_code_to_kr_isin(short_code: str) -> str:
    code = f"KR7{str(short_code).strip()}00"
    converted = ""
    for char in code:
        if char.isdigit():
            converted += char
        else:
            converted += str(ord(char.upper()) - 55)

    digits = []
    for index, char in enumerate(converted):
        digit = int(char)
        if index % 2 == 0:
            doubled = digit * 2
            digits.extend(int(part) for part in str(doubled))
        else:
            digits.append(digit)

    total = sum(digits)
    check_digit = (10 - (total % 10)) % 10
    return f"{code}{check_digit}"


def fetch_tiger_holdings(
    session: requests.Session,
    etf_name: str,
    manager: str,
    short_code: str,
    fund_code: str,
) -> tuple[str, str | None, pd.DataFrame]:
    ksd_fund = short_code_to_kr_isin(short_code)
    detail_url = TIGER_DETAIL_URL.format(ksd_fund=ksd_fund)

    response = session.post(TIGER_PDF_URL, data={"ksdFund": ksd_fund}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    fix_date_input = soup.select_one("input[name='fixDate']")
    prf_prd_select = soup.select_one("select[name='prfPrd'] option[selected]")
    fix_date = fix_date_input.get("value", "").replace(".", "") if fix_date_input else ""
    prf_prd = prf_prd_select.get("value", "Week01") if prf_prd_select else "Week01"
    asof_date = None
    if fix_date:
        try:
            asof_date = datetime.strptime(fix_date, "%Y%m%d").date().isoformat()
        except ValueError:
            asof_date = None

    items_response = session.get(
        TIGER_ITEMS_URL,
        params={
            "ksdFund": ksd_fund,
            "prfPrd": prf_prd,
            "fixDate": fix_date,
            "listCnt": 100,
        },
        timeout=REQUEST_TIMEOUT,
    )
    items_response.raise_for_status()
    payload = items_response.json()
    rows = payload.get("rtnData") or []
    if not rows:
        raise RuntimeError("TIGER item list was empty")

    holdings_rows = []
    for item in rows:
        name = str(item.get("memItemname") or item.get("memItemnameEng") or "").strip()
        weight = item.get("stockRate")
        if not name or weight is None:
            continue
        holdings_rows.append(
            {
                "manager": manager,
                "etf_name": etf_name,
                "short_code": short_code,
                "fund_code": fund_code,
                "holding_name": name,
                "weight_pct": float(weight),
                "asof_date": asof_date,
                "source": "TIGER",
            }
        )

    holdings_df = pd.DataFrame(holdings_rows)
    if holdings_df.empty:
        raise RuntimeError("TIGER holdings rows were empty")

    return detail_url, asof_date, holdings_df
