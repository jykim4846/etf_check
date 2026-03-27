from __future__ import annotations

import io
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"
DATA_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

FUNETF_FILTER_EXCEL_URL = "https://www.funetf.co.kr/api/public/download/excel/etfFilter"
FUNETF_DETAIL_URL = "https://www.funetf.co.kr/product/etf/view/{fund_code}"
TIME_LINEUP_URL = "https://timeetf.co.kr/m11.php"
TIGER_ACTIVE_LINEUP_URL = "https://investments.miraeasset.com/tigeretf/ko/content/activeLineUp/list.do"
KODEX_LIST_URL = "https://www.samsungfund.com/etf/product/list.do"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8"}

MANAGER_RULES = {
    "삼성": ["삼성", "samsung"],
    "미래에셋": ["미래", "mirae"],
    "타임폴리오": ["타임", "timefolio", "time"],
}

MAX_ETFS = int(os.getenv("MAX_ETFS", "0") or "0")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30") or "30")
KST = timezone(timedelta(hours=9))

THEME_RULES = {
    "AI": ["ai", "인공지능", "nasdaq ai", "aiplatform"],
    "반도체": ["반도체", "semiconductor", "chip"],
    "바이오": ["바이오", "bio", "헬스케어", "healthcare", "제약", "치료제"],
    "배당": ["배당", "dividend"],
    "커버드콜": ["커버드콜", "covered call", "buffer"],
    "금리형": ["금리", "머니마켓", "mmf", "sofr", "kofr", "cd", "파킹형"],
    "채권": ["채권", "국고채", "회사채", "금융채", "bond"],
    "리츠/부동산": ["리츠", "부동산", "인프라"],
    "원자재": ["금", "gold", "원유", "oil", "천연가스", "gas", "원자재"],
    "전력/에너지": ["전력", "에너지", "ess", "친환경", "신재생", "전력인프라"],
    "방산/우주": ["방산", "우주", "aerospace", "defense"],
    "로봇": ["로봇", "robot"],
    "모빌리티": ["자율주행", "모빌리티", "2차전지", "전지", "배터리"],
    "중국": ["차이나", "china"],
    "미국": ["미국", "us ", "s&p500", "나스닥", "dow", "dow jones"],
}


@dataclass
class ETFRecord:
    manager: str
    etf_name: str
    short_code: str
    fund_code: str
    detail_url: str
    source: str = "FunETF"


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


def normalize_columns(columns: Iterable[object]) -> list[str]:
    normalized: list[str] = []
    for col in columns:
        text = str(col).strip().replace("\n", " ")
        text = re.sub(r"\s+", " ", text)
        normalized.append(text)
    return normalized


def manager_from_text(*values: object) -> str | None:
    haystack = " ".join(str(v).lower() for v in values if v is not None)
    for manager, tokens in MANAGER_RULES.items():
        if any(token in haystack for token in tokens):
            return manager
    return None


def find_column(df: pd.DataFrame, candidates: list[str], default_idx: int | None = None) -> str:
    lower_map = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        cand = candidate.lower()
        for lower_col, original in lower_map.items():
            if cand == lower_col or cand in lower_col:
                return original
    if default_idx is not None and 0 <= default_idx < len(df.columns):
        return df.columns[default_idx]
    raise KeyError(f"Column not found. Candidates={candidates}, columns={list(df.columns)}")


def compact_text(*values: object) -> str:
    return " ".join(str(v).strip() for v in values if v is not None and str(v).strip() and str(v).strip() != "nan")


def classify_asset_class(
    etf_type: str,
    etf_subtype: str,
    representative_main: str,
    representative_sub: str,
    etf_name: str,
) -> str:
    text = compact_text(etf_type, etf_subtype, representative_main, representative_sub, etf_name).lower()
    if any(token in text for token in ["채권", "국고채", "회사채", "금융채"]):
        return "채권"
    if any(token in text for token in ["머니마켓", "금리", "파킹형", "cd", "kofr", "sofr"]):
        return "금리형"
    if any(token in text for token in ["원자재", "금", "원유", "천연가스", "팔라듐"]):
        return "원자재"
    if any(token in text for token in ["부동산", "리츠", "인프라"]):
        return "부동산/리츠"
    if any(token in text for token in ["혼합", "tif"]):
        return "혼합자산"
    if any(token in text for token in ["주식", "테크", "성장", "배당", "바이오", "반도체", "코스피", "코스닥", "나스닥", "s&p"]):
        return "주식"
    return "기타"


def classify_style(replica_type: str, etf_type: str, etf_name: str) -> str:
    text = compact_text(replica_type, etf_type, etf_name).lower()
    if "레버리지" in text:
        return "레버리지"
    if "인버스" in text:
        return "인버스"
    if "커버드콜" in text:
        return "커버드콜"
    if "버퍼" in text:
        return "버퍼형"
    if "액티브" in text:
        return "액티브"
    return "기타"


def classify_theme(etf_name: str, representative_sub: str, benchmark_name: str) -> str:
    text = compact_text(etf_name, representative_sub, benchmark_name).lower()
    for theme, tokens in THEME_RULES.items():
        if any(token in text for token in tokens):
            return theme
    return "기타"


def build_category_tags(asset_class: str, style: str, theme: str, representative_sub: str) -> str:
    tags = [asset_class, style, theme]
    if representative_sub and representative_sub != "nan":
        tags.append(representative_sub)
    return " | ".join(dict.fromkeys(tag for tag in tags if tag and tag != "기타"))


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

    records = []
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
        theme = classify_theme(etf_name, str(row[representative_sub_col]), str(row[benchmark_col]))
        category_tags = build_category_tags(asset_class, style, theme, str(row[representative_sub_col]))

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


def parse_float(text: str | None) -> float | None:
    if text is None:
        return None
    found = re.search(r"-?\d[\d,]*\.?\d*", text.replace("%", ""))
    if not found:
        return None
    try:
        return float(found.group(0).replace(",", ""))
    except ValueError:
        return None


def extract_aum(text: str) -> tuple[float | None, str | None]:
    patterns = [
        r"순자산총액\s*([\d,]+\.?\d*)\s*억원",
        r"순자산\s*([\d,]+\.?\d*)\s*억원",
        r"운용규모\s*([\d,]+\.?\d*)\s*억원",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            value = float(m.group(1).replace(",", ""))
            return value, "억원"
    return None, None


def extract_asof(text: str) -> str | None:
    m = re.search(r"(\d{2}\.\d{2}\.\d{2}|\d{4}\.\d{2}\.\d{2})\s*기준", text)
    if not m:
        return None
    token = m.group(1)
    if len(token.split(".")[0]) == 2:
        token = f"20{token}"
    try:
        return datetime.strptime(token, "%Y.%m.%d").date().isoformat()
    except ValueError:
        return None


def score_holdings_table(df: pd.DataFrame) -> int:
    cols = [str(c) for c in df.columns]
    score = 0
    if any("종목명" in c for c in cols):
        score += 5
    if any("비중" in c for c in cols):
        score += 5
    if len(df) >= 3:
        score += 2
    return score


def parse_holdings_tables(html: str) -> list[pd.DataFrame]:
    tables: list[pd.DataFrame] = []
    try:
        for df in pd.read_html(io.StringIO(html)):
            df.columns = normalize_columns(df.columns)
            if score_holdings_table(df) >= 10:
                tables.append(df)
    except ValueError:
        pass
    return tables


def tidy_holdings_table(df: pd.DataFrame) -> pd.DataFrame:
    name_col = find_column(df, ["종목명", "보유종목"], default_idx=0)
    weight_col = find_column(df, ["비중(%)", "비중 %", "비중"], default_idx=1)

    out = pd.DataFrame({
        "holding_name": df[name_col].astype(str).str.strip(),
        "weight_pct": df[weight_col].apply(lambda v: parse_float(str(v))),
    })
    out = out[out["holding_name"].ne("") & out["holding_name"].ne("nan")]
    out = out[out["weight_pct"].notna()]
    out = out[~out["holding_name"].str.contains("종목명|보유종목|업종", na=False)]
    out = out.drop_duplicates(subset=["holding_name", "weight_pct"]).reset_index(drop=True)
    return out


def fetch_etf_detail(session: requests.Session, record: ETFRecord) -> tuple[dict, pd.DataFrame]:
    html = fetch_text(session, record.detail_url)
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    aum_okr, aum_unit = extract_aum(text)
    asof_date = extract_asof(text)

    summary = {
        "manager": record.manager,
        "etf_name": record.etf_name,
        "short_code": record.short_code,
        "fund_code": record.fund_code,
        "detail_url": record.detail_url,
        "source": record.source,
        "aum_okr": aum_okr,
        "aum_unit": aum_unit,
        "asof_date": asof_date,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    tables = parse_holdings_tables(html)
    holdings_frames: list[pd.DataFrame] = []
    for table in tables:
        try:
            cleaned = tidy_holdings_table(table)
        except Exception:
            continue
        if not cleaned.empty:
            holdings_frames.append(cleaned)

    if holdings_frames:
        holdings = max(holdings_frames, key=len).copy()
    else:
        snippet_matches = re.findall(r"([가-힣A-Za-z0-9&().,/+\- ]{2,})\s+([\-]?[0-9]+(?:\.[0-9]+)?)", text)
        rows = []
        for name, weight in snippet_matches[:20]:
            if any(bad in name for bad in ["순위", "비중", "종목명", "운용", "수익률"]):
                continue
            parsed = parse_float(weight)
            if parsed is None:
                continue
            rows.append({"holding_name": name.strip(), "weight_pct": parsed})
        holdings = pd.DataFrame(rows).drop_duplicates().head(10)

    if holdings.empty:
        holdings = pd.DataFrame(columns=["holding_name", "weight_pct"])

    holdings.insert(0, "fund_code", record.fund_code)
    holdings.insert(0, "short_code", record.short_code)
    holdings.insert(0, "etf_name", record.etf_name)
    holdings.insert(0, "manager", record.manager)
    holdings["asof_date"] = asof_date
    holdings["source"] = record.source
    return summary, holdings


def build_html(etf_df: pd.DataFrame, holdings_df: pd.DataFrame) -> str:
    etf_json = json.dumps(etf_df.fillna("").to_dict(orient="records"), ensure_ascii=False)
    holdings_json = json.dumps(holdings_df.fillna("").to_dict(orient="records"), ensure_ascii=False)
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ETF Check</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #f7f7f8; color: #111; }}
    .wrap {{ max-width: 1100px; margin: 0 auto; padding: 20px; }}
    .card {{ background: white; border-radius: 14px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.08); margin-bottom: 16px; }}
    h1 {{ margin: 0 0 6px; font-size: 24px; }}
    .muted {{ color: #666; font-size: 14px; }}
    .filters {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }}
    .filter-block {{ margin-top: 12px; }}
    .filter-title {{ font-size: 12px; color: #666; margin-bottom: 6px; }}
    button {{ border: 1px solid #ddd; background: white; border-radius: 999px; padding: 8px 12px; cursor: pointer; }}
    button.active {{ background: #111; color: white; border-color: #111; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #eee; text-align: left; padding: 10px 8px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: #fff; }}
    .num {{ text-align: right; white-space: nowrap; }}
    .small {{ font-size: 12px; color: #666; }}
    a {{ color: inherit; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>액티브 ETF 체크</h1>
      <div class="muted">삼성(KODEX), 미래에셋(TIGER), 타임폴리오(TIME) 액티브 ETF를 순자산총액(AUM) 기준으로 정렬합니다. 업데이트: {updated_at}</div>
      <div class="filter-block" id="manager-filters"></div>
      <div class="filter-block" id="asset-filters"></div>
      <div class="filter-block" id="theme-filters"></div>
    </div>
    <div class="card">
      <table id="etf-table">
        <thead>
          <tr>
            <th>운용사</th>
            <th>ETF</th>
            <th>카테고리</th>
            <th class="num">AUM(억원)</th>
            <th>기준일</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="card">
      <h2 style="margin-top:0;font-size:18px;">구성종목</h2>
      <div class="small" id="selected-name">ETF를 선택하면 구성종목이 표시됩니다.</div>
      <table id="holdings-table">
        <thead>
          <tr>
            <th>종목명</th>
            <th class="num">비중(%)</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
    </div>
  </div>
<script>
const etfs = {etf_json};
const holdings = {holdings_json};
let currentManager = '전체';
let currentAssetClass = '전체';
let currentTheme = '전체';
let currentFundCode = etfs.length ? etfs[0].fund_code : null;

function formatNum(v) {{
  if (v === '' || v === null || v === undefined) return '-';
  const n = Number(v);
  if (Number.isNaN(n)) return String(v);
  return n.toLocaleString('ko-KR', {{ maximumFractionDigits: 2 }});
}}

function uniqueOptions(key) {{
  return ['전체', ...new Set(etfs.map(x => x[key]).filter(Boolean))];
}}

function filteredEtfs() {{
  const rows = etfs.filter(x => {{
    if (currentManager !== '전체' && x.manager !== currentManager) return false;
    if (currentAssetClass !== '전체' && x.asset_class !== currentAssetClass) return false;
    if (currentTheme !== '전체' && x.theme !== currentTheme) return false;
    return true;
  }});
  return [...rows].sort((a, b) => Number(b.aum_okr || -1) - Number(a.aum_okr || -1));
}}

function renderFilterGroup(rootId, title, currentValue, options, onSelect) {{
  const root = document.getElementById(rootId);
  root.innerHTML = '';
  const label = document.createElement('div');
  label.className = 'filter-title';
  label.textContent = title;
  root.appendChild(label);

  const wrap = document.createElement('div');
  wrap.className = 'filters';
  options.forEach(m => {{
    const btn = document.createElement('button');
    btn.textContent = m;
    if (m === currentValue) btn.classList.add('active');
    btn.onclick = () => onSelect(m);
    wrap.appendChild(btn);
  }});
  root.appendChild(wrap);
}}

function rerenderAfterFilter() {{
  const rows = filteredEtfs();
  currentFundCode = rows.length ? rows[0].fund_code : null;
  renderFilters();
  renderEtfs();
  renderHoldings();
}}

function renderFilters() {{
  renderFilterGroup('manager-filters', '운용사', currentManager, uniqueOptions('manager'), value => {{
    currentManager = value;
    rerenderAfterFilter();
  }});
  renderFilterGroup('asset-filters', '자산군', currentAssetClass, uniqueOptions('asset_class'), value => {{
    currentAssetClass = value;
    rerenderAfterFilter();
  }});
  renderFilterGroup('theme-filters', '테마', currentTheme, uniqueOptions('theme'), value => {{
    currentTheme = value;
    rerenderAfterFilter();
  }});
}}

function renderEtfs() {{
  const tbody = document.querySelector('#etf-table tbody');
  tbody.innerHTML = '';
  filteredEtfs().forEach(row => {{
    const tr = document.createElement('tr');
    tr.style.cursor = 'pointer';
    tr.onclick = () => {{ currentFundCode = row.fund_code; renderEtfs(); renderHoldings(); }};
    if (row.fund_code === currentFundCode) tr.style.background = '#fafafa';
    tr.innerHTML = `
      <td>${{row.manager}}</td>
      <td><strong>${{row.etf_name}}</strong><div class="small">${{row.short_code}}</div></td>
      <td><div>${{row.asset_class || '-'}}</div><div class="small">${{row.style || '-'}} / ${{row.theme || '-'}}</div></td>
      <td class="num">${{formatNum(row.aum_okr)}}</td>
      <td>${{row.asof_date || '-'}}</td>
      <td>${{row.detail_url ? '<a href="' + row.detail_url + '" target="_blank" rel="noreferrer">원본</a>' : '-'}}</td>`;
    tbody.appendChild(tr);
  }});
}}

function renderHoldings() {{
  const tbody = document.querySelector('#holdings-table tbody');
  tbody.innerHTML = '';
  const selected = etfs.find(x => x.fund_code === currentFundCode);
  document.getElementById('selected-name').textContent = selected ? `${{selected.etf_name}} / 기준일: ${{selected.asof_date || '-'}} / 카테고리: ${{selected.category_tags || '-'}}` : '선택된 ETF가 없습니다.';
  const rows = holdings.filter(x => x.fund_code === currentFundCode).sort((a,b) => Number(b.weight_pct || -1) - Number(a.weight_pct || -1));
  rows.forEach(row => {{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${{row.holding_name}}</td><td class="num">${{formatNum(row.weight_pct)}}</td>`;
    tbody.appendChild(tr);
  }});
}}

renderFilters();
renderEtfs();
renderHoldings();
</script>
</body>
</html>
"""


def main() -> None:
    session = session_with_retries()

    etf_df = load_etf_universe(session)
    if etf_df.empty:
        raise RuntimeError("액티브 ETF 목록을 찾지 못했습니다. 사이트 구조가 바뀌었는지 확인하세요.")

    holdings_frames: list[pd.DataFrame] = []
    for row in etf_df.to_dict(orient="records"):
        holdings = build_holdings_from_row(pd.Series(row))
        holdings_frames.append(holdings)
        print(f"[OK] {row['etf_name']}")

    summary_df = etf_df.copy()
    summary_df["fetched_at_utc"] = datetime.now(timezone.utc).isoformat()
    summary_df["error"] = ""
    summary_df["run_date_kst"] = datetime.now(timezone.utc).astimezone(KST).isoformat()
    summary_df = summary_df.sort_values(by=["aum_okr", "manager", "etf_name"], ascending=[False, True, True], na_position="last")
    summary_df = summary_df[
        ["manager", "etf_name", "short_code", "fund_code", "detail_url", "source", "asset_class", "style", "theme", "category_tags", "aum_okr", "aum_unit", "asof_date", "fetched_at_utc", "error", "run_date_kst"]
    ]

    if holdings_frames:
        holdings_df = pd.concat(holdings_frames, ignore_index=True)
    else:
        holdings_df = pd.DataFrame(columns=["manager", "etf_name", "short_code", "fund_code", "holding_name", "weight_pct", "asof_date", "source"])

    summary_df.to_csv(DATA_DIR / "etf_list.csv", index=False, encoding="utf-8-sig")
    holdings_df.to_csv(DATA_DIR / "etf_holdings.csv", index=False, encoding="utf-8-sig")

    (DATA_DIR / "etf_list.json").write_text(summary_df.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
    (DATA_DIR / "etf_holdings.json").write_text(holdings_df.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")

    (DOCS_DIR / "index.html").write_text(build_html(summary_df, holdings_df), encoding="utf-8")

    readme_note = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "etf_filter_excel": FUNETF_FILTER_EXCEL_URL,
            "time_lineup": TIME_LINEUP_URL,
            "tiger_active_lineup": TIGER_ACTIVE_LINEUP_URL,
            "kodex_list": KODEX_LIST_URL,
        },
        "note": "AUM과 구성종목은 FunETF/공식 운용사 공개 페이지 기준이며, 장중 실시간 값과 차이날 수 있습니다.",
    }
    (DATA_DIR / "meta.json").write_text(json.dumps(readme_note, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
