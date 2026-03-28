from __future__ import annotations

from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from config import REQUEST_TIMEOUT, TIME_LINEUP_URL


def _normalize_name(value: str) -> str:
    return " ".join(str(value).split()).strip().lower()


def load_time_lineup(session: requests.Session) -> dict[str, str]:
    response = session.get(TIME_LINEUP_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    lineup: dict[str, str] = {}
    for link in soup.select("a[href*='m11_view.php?idx=']"):
        title_div = link.select_one(".tit div")
        if title_div is None:
            continue
        label = " ".join(title_div.get_text(" ", strip=True).split())
        if not label:
            continue
        href = link.get("href", "").strip()
        if not href:
            continue
        detail_url = urljoin(TIME_LINEUP_URL, href)
        lineup[_normalize_name(f"TIME {label}")] = detail_url
        lineup[_normalize_name(label)] = detail_url
    return lineup


def fetch_time_holdings(
    session: requests.Session,
    etf_name: str,
    manager: str,
    short_code: str,
    fund_code: str,
) -> tuple[str, str | None, pd.DataFrame]:
    lineup = load_time_lineup(session)
    detail_url = lineup.get(_normalize_name(etf_name))
    if not detail_url:
        raise RuntimeError("TIME ETF official detail page not found")

    response = session.get(detail_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    pdf_date = soup.select_one("#pdfDate")
    asof_date = pdf_date.get("value") if pdf_date else None

    table = soup.select_one("#constituentItems table tbody")
    if table is None:
        raise RuntimeError("TIME ETF holdings table not found")

    rows: list[dict] = []
    for tr in table.select("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.select("td")]
        if len(cells) < 5:
            continue
        _, holding_name, _, _, weight_text = cells[:5]
        weight_text = weight_text.replace(",", "").replace("%", "").strip()
        try:
            weight_pct = float(weight_text)
        except ValueError:
            continue

        rows.append(
            {
                "manager": manager,
                "etf_name": etf_name,
                "short_code": short_code,
                "fund_code": fund_code,
                "holding_name": holding_name,
                "weight_pct": weight_pct,
                "asof_date": asof_date,
                "source": "TIME",
            }
        )

    holdings_df = pd.DataFrame(rows)
    if holdings_df.empty:
        raise RuntimeError("TIME ETF holdings table was empty")

    return detail_url, asof_date, holdings_df
