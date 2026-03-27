from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable

import pandas as pd

from config import MANAGER_RULES


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
    return " ".join(
        str(v).strip()
        for v in values
        if v is not None and str(v).strip() and str(v).strip() != "nan"
    )


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
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = float(match.group(1).replace(",", ""))
            return value, "억원"
    return None, None


def extract_asof(text: str) -> str | None:
    match = re.search(r"(\d{2}\.\d{2}\.\d{2}|\d{4}\.\d{2}\.\d{2})\s*기준", text)
    if not match:
        return None
    token = match.group(1)
    if len(token.split(".")[0]) == 2:
        token = f"20{token}"
    try:
        return datetime.strptime(token, "%Y.%m.%d").date().isoformat()
    except ValueError:
        return None
