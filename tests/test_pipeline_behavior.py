import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

import main
import outputs.files as output_files
import pipeline
from collectors.timeetf import load_time_lineup
from collectors.kodex import search_kodex_fid
from collectors.tiger import short_code_to_kr_isin


class ClassificationTests(unittest.TestCase):
    def test_classify_asset_class_bond(self):
        self.assertEqual(
            main.classify_asset_class("채권형", "", "", "", "어떤 ETF"),
            "채권",
        )

    def test_classify_style_active(self):
        self.assertEqual(main.classify_style("액티브", "", ""), "액티브")

    def test_classify_theme_ai(self):
        self.assertEqual(main.classify_theme("KODEX AI전력핵심설비액티브", "", ""), "AI")

    def test_is_overseas_etf_true(self):
        self.assertTrue(
            main.is_overseas_etf("", "", "", "", "미국 AI 액티브", "NASDAQ AI Index")
        )


class HoldingsBuilderTests(unittest.TestCase):
    def test_build_holdings_from_row_uses_top_columns(self):
        row = pd.Series(
            {
                "manager": "삼성",
                "etf_name": "테스트 ETF",
                "short_code": "123456",
                "fund_code": "FUND123",
                "asof_date": "2026-03-27",
                "source": "FunETF",
                "top_1": "삼성전자",
                "top_1_weight_pct": 10.5,
                "top_2": "SK하이닉스",
                "top_2_weight_pct": 9.2,
            }
        )
        df = main.build_holdings_from_row(row)
        self.assertEqual(list(df["holding_name"]), ["삼성전자", "SK하이닉스"])
        self.assertEqual(list(df["weight_pct"]), [10.5, 9.2])

    def test_build_holdings_from_api_uses_api_rows(self):
        row = pd.Series(
            {
                "manager": "미래에셋",
                "etf_name": "테스트 ETF",
                "short_code": "654321",
                "fund_code": "FUND999",
                "asof_date": "2026-03-27",
                "source": "FunETF",
            }
        )
        df = main.build_holdings_from_api(
            row,
            [
                {"citmNm": "알테오젠", "evP": "4.25"},
                {"citmNm": "삼천당제약", "evP": 3.1},
            ],
        )
        self.assertEqual(list(df["holding_name"]), ["알테오젠", "삼천당제약"])
        self.assertEqual(list(df["weight_pct"]), [4.25, 3.1])


class HtmlOutputTests(unittest.TestCase):
    def test_build_html_contains_filter_sections(self):
        etf_df = pd.DataFrame(
            [
                {
                    "manager": "삼성",
                    "etf_name": "테스트 ETF",
                    "short_code": "123456",
                    "fund_code": "FUND123",
                    "detail_url": "https://example.com",
                    "source": "FunETF",
                    "asset_class": "주식",
                    "style": "액티브",
                    "theme": "AI",
                    "category_tags": "주식 | 액티브 | AI",
                    "aum_okr": 123.45,
                    "aum_unit": "억원",
                    "asof_date": "2026-03-27",
                    "holdings_source": "FunETF",
                    "holding_count": 1,
                    "fetched_at_utc": "2026-03-27T00:00:00+00:00",
                    "error": "",
                    "run_date_kst": "2026-03-27T09:00:00+09:00",
                }
            ]
        )
        holdings_df = pd.DataFrame(
            [
                {
                    "manager": "삼성",
                    "etf_name": "테스트 ETF",
                    "short_code": "123456",
                    "fund_code": "FUND123",
                    "holding_name": "삼성전자",
                    "weight_pct": 12.34,
                    "asof_date": "2026-03-27",
                    "source": "FunETF",
                }
            ]
        )
        html = main.build_html(etf_df, holdings_df)
        self.assertIn("manager-filters", html)
        self.assertIn("테스트 ETF", html)
        self.assertIn("최근 수집 상태", html)
        self.assertIn("badge.svg", html)
        self.assertIn("보유종목", html)
        self.assertIn("fallback", html)
        self.assertIn("바이오 보유종목 비교 Top 10", html)


class TimeOfficialSourceTests(unittest.TestCase):
    def test_load_time_lineup_maps_full_name_to_detail_url(self):
        html = """
        <html><body>
          <a href="./m11_view.php?idx=13">
            <div class="tit"><strong>TIME</strong><div>K바이오액티브</div></div>
          </a>
        </body></html>
        """

        class DummyResponse:
            def __init__(self, text):
                self.text = text
            def raise_for_status(self):
                return None

        class DummySession:
            def get(self, url, timeout=None):
                return DummyResponse(html)

        lineup = load_time_lineup(DummySession())
        self.assertIn("time k바이오액티브", lineup)
        self.assertTrue(lineup["time k바이오액티브"].endswith("m11_view.php?idx=13"))


class TigerOfficialSourceTests(unittest.TestCase):
    def test_short_code_to_kr_isin(self):
        self.assertEqual(short_code_to_kr_isin("329750"), "KR7329750004")


class KodexOfficialSourceTests(unittest.TestCase):
    def test_search_kodex_fid_prefers_short_code_match(self):
        class DummyResponse:
            def raise_for_status(self):
                return None

            def json(self):
                return [
                    {"stkTicker": "445290", "fId": "2ETFH5", "fNm": "KODEX 로봇액티브"}
                ]

        class DummySession:
            def get(self, url, params=None, timeout=None, headers=None):
                return DummyResponse()

        fid, official_name = search_kodex_fid(DummySession(), "KODEX 로봇액티브", "445290")
        self.assertEqual(fid, "2ETFH5")
        self.assertEqual(official_name, "KODEX 로봇액티브")


class MainPipelineTests(unittest.TestCase):
    def test_main_writes_outputs_and_preserves_error_column(self):
        summary = pd.DataFrame(
            [
                {
                    "manager": "삼성",
                    "etf_name": "테스트 ETF",
                    "short_code": "123456",
                    "fund_code": "FUND123",
                    "detail_url": "",
                    "source": "FunETF",
                    "asset_class": "주식",
                    "style": "액티브",
                    "theme": "AI",
                    "category_tags": "주식 | 액티브 | AI",
                    "aum_okr": 123.45,
                    "aum_unit": "억원",
                    "asof_date": "2026-03-27",
                    "top_1": "삼성전자",
                    "top_1_weight_pct": 10.0,
                }
            ]
        )

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            data_dir = tmp_path / "data"
            docs_dir = tmp_path / "docs"
            data_dir.mkdir()
            docs_dir.mkdir()

            with patch.object(output_files, "DATA_DIR", data_dir), \
                 patch.object(output_files, "DOCS_DIR", docs_dir), \
                 patch.object(pipeline, "load_etf_universe", return_value=summary), \
                 patch.object(pipeline, "resolve_item_id", return_value=(None, "")):
                main.main()

            output = pd.read_csv(data_dir / "etf_list.csv")
            self.assertIn("error", output.columns)
            self.assertIn("holding_count", output.columns)
            self.assertIn("holdings_source", output.columns)
            self.assertEqual(output.loc[0, "etf_name"], "테스트 ETF")
            self.assertIn("item_id_not_found", str(output.loc[0, "error"]))
            summary = pd.read_json(data_dir / "run_summary.json", typ="series")
            self.assertEqual(int(summary["etf_count"]), 1)
            self.assertEqual(int(summary["error_etf_count"]), 1)


if __name__ == "__main__":
    unittest.main()
