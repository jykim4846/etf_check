from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ETFRecord:
    manager: str
    etf_name: str
    short_code: str
    fund_code: str
    detail_url: str
    source: str = "FunETF"
