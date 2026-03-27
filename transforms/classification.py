from __future__ import annotations

from config import THEME_RULES
from transforms.parsing import compact_text


def classify_asset_class(
    etf_type: str,
    etf_subtype: str,
    representative_main: str,
    representative_sub: str,
    etf_name: str,
) -> str:
    text = compact_text(
        etf_type, etf_subtype, representative_main, representative_sub, etf_name
    ).lower()
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
    if any(
        token in text
        for token in ["주식", "테크", "성장", "배당", "바이오", "반도체", "코스피", "코스닥", "나스닥", "s&p"]
    ):
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


def is_overseas_etf(
    etf_type: str,
    etf_subtype: str,
    representative_main: str,
    representative_sub: str,
    etf_name: str,
    benchmark_name: str,
) -> bool:
    combined = compact_text(
        etf_type, etf_subtype, representative_main, representative_sub, etf_name, benchmark_name
    ).lower()
    explicit_tokens = [
        "해외주식",
        "해외채권",
        "글로벌",
        "미국",
        "차이나",
        "중국",
        "아시아",
        "토탈월드",
        "나스닥",
        "s&p",
        "dow jones",
        "dow ",
        "msci us",
        "u.s.",
        "us treasury",
        "ice u.s.",
        "solactive global",
        "bloomberg greater china",
    ]
    return any(token in combined for token in explicit_tokens)
