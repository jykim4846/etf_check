from __future__ import annotations

import os
from datetime import timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DOCS_DIR = BASE_DIR / "docs"

DATA_DIR.mkdir(exist_ok=True)
DOCS_DIR.mkdir(exist_ok=True)

FUNETF_FILTER_EXCEL_URL = "https://www.funetf.co.kr/api/public/download/excel/etfFilter"
FUNETF_DETAIL_URL = "https://www.funetf.co.kr/product/etf/view/{fund_code}"
FUNETF_SEARCH_URL = "https://www.funetf.co.kr/search"
FUNETF_SEARCH_API_URL = "https://www.funetf.co.kr/api/public/main/search/all"
FUNETF_PDF_API_URL = "https://www.funetf.co.kr/api/public/product/view/etfpdf"

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

MAX_ETFS = int(os.getenv("MAX_ETFS", "0") or "0")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30") or "30")
KST = timezone(timedelta(hours=9))

BIO_TARGETS = [
    {"manager": "삼성", "etf_name": "KoAct 바이오헬스케어액티브", "short_code": "462900"},
    {"manager": "미래에셋", "etf_name": "TIGER 기술이전바이오액티브", "short_code": "0168K0"},
    {"manager": "타임폴리오", "etf_name": "TIME K바이오액티브", "short_code": "463050"},
]

BIO_TARGET_SHORT_CODES = {item["short_code"] for item in BIO_TARGETS}
