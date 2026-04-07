import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

import main
from collectors.funetf import load_etf_universe
import outputs.files as output_files
import pipeline
from collectors.timeetf import load_time_lineup
from collectors.kodex import search_kodex_fid
from collectors.tiger import short_code_to_kr_isin
from outputs.files import build_holding_changes


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
        self.assertIn("asset-filters", html)
        self.assertIn("theme-filters", html)
        self.assertIn("테스트 ETF", html)
        self.assertIn("수동 재수집 실행", html)
        self.assertIn("보유종목", html)
        self.assertIn("바이오 종목별 편입 비중", html)
        self.assertIn("직전 대비 변동", html)


class HoldingChangeTests(unittest.TestCase):
    def test_build_holding_changes_marks_delta_states(self):
        current_df = pd.DataFrame(
            [
                {
                    "manager": "운용사",
                    "etf_name": "ETF A",
                    "short_code": "111111",
                    "fund_code": "FUNDA",
                    "holding_name": "알테오젠",
                    "weight_pct": 12.0,
                    "asof_date": "2026-04-06",
                    "source": "FunETF",
                },
                {
                    "manager": "운용사",
                    "etf_name": "ETF A",
                    "short_code": "111111",
                    "fund_code": "FUNDA",
                    "holding_name": "리가켐바이오",
                    "weight_pct": 4.0,
                    "asof_date": "2026-04-06",
                    "source": "FunETF",
                },
            ]
        )
        previous_df = pd.DataFrame(
            [
                {
                    "manager": "운용사",
                    "etf_name": "ETF A",
                    "short_code": "111111",
                    "fund_code": "FUNDA",
                    "holding_name": "알테오젠",
                    "weight_pct": 10.0,
                    "asof_date": "2026-04-05",
                    "source": "FunETF",
                },
                {
                    "manager": "운용사",
                    "etf_name": "ETF A",
                    "short_code": "111111",
                    "fund_code": "FUNDA",
                    "holding_name": "삼천당제약",
                    "weight_pct": 3.5,
                    "asof_date": "2026-04-05",
                    "source": "FunETF",
                },
            ]
        )

        enriched_df, changes_df = build_holding_changes(current_df, previous_df)

        increased = enriched_df.loc[enriched_df["holding_name"] == "알테오젠"].iloc[0]
        added = enriched_df.loc[enriched_df["holding_name"] == "리가켐바이오"].iloc[0]
        removed = changes_df.loc[changes_df["holding_name"] == "삼천당제약"].iloc[0]

        self.assertEqual(increased["change_state"], "증가")
        self.assertEqual(increased["weight_diff_pct"], 2.0)
        self.assertEqual(added["change_state"], "신규")
        self.assertTrue(pd.isna(added["previous_weight_pct"]))
        self.assertEqual(removed["change_state"], "제외")
        self.assertEqual(removed["current_weight_pct"], 0.0)
        self.assertEqual(removed["weight_diff_pct"], -3.5)

    def test_build_holding_changes_accepts_previous_current_weight_schema(self):
        current_df = pd.DataFrame(
            [
                {
                    "manager": "운용사",
                    "etf_name": "ETF A",
                    "short_code": "111111",
                    "fund_code": "FUNDA",
                    "holding_name": "알테오젠",
                    "weight_pct": 12.0,
                    "asof_date": "2026-04-08",
                    "source": "FunETF",
                }
            ]
        )
        previous_df = pd.DataFrame(
            [
                {
                    "manager": "운용사",
                    "etf_name": "ETF A",
                    "short_code": "111111",
                    "fund_code": "FUNDA",
                    "holding_name": "알테오젠",
                    "current_weight_pct": 10.5,
                    "previous_weight_pct": 10.0,
                    "weight_diff_pct": 0.5,
                    "change_state": "증가",
                    "asof_date": "2026-04-07",
                    "source": "FunETF",
                }
            ]
        )

        enriched_df, _ = build_holding_changes(current_df, previous_df)

        row = enriched_df.iloc[0]
        self.assertEqual(row["previous_weight_pct"], 10.5)
        self.assertEqual(row["weight_diff_pct"], 1.5)
        self.assertEqual(row["change_state"], "증가")


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


class FunEtfUniverseTests(unittest.TestCase):
    def test_load_etf_universe_tolerates_missing_schema_columns(self):
        df = pd.DataFrame(
            [
                {
                    "ETF 종목명": "KODEX 로봇액티브",
                    "ETF 단축코드": "445290",
                    "ETF 대유형": "국내주식형",
                    "운용규모(억원)": "9856.35",
                    "운용사명": "삼성자산운용",
                    "TOP 1": "삼성전자",
                    "비율(%)": "10.5",
                    "TOP 2": "SK하이닉스",
                    "비율(%).1": "8.1",
                    "TOP 3": "한화에어로스페이스",
                    "비율(%).2": "7.2",
                },
                {
                    "ETF 종목명": "ACE 바이오TOP10액티브",
                    "ETF 단축코드": "999999",
                    "ETF 대유형": "국내주식형",
                    "운용규모(억원)": "500",
                    "운용사명": "한국투자신탁운용",
                    "TOP 1": "알테오젠",
                    "비율(%)": "11.5",
                },
                {
                    "ETF 종목명": "KODEX 200",
                    "ETF 단축코드": "069500",
                    "ETF 대유형": "국내주식형",
                    "운용규모(억원)": "1000",
                    "운용사명": "삼성자산운용",
                },
            ]
        )
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)

        class DummySession:
            pass

        with patch("collectors.funetf.fetch_bytes", return_value=excel_buffer.getvalue()):
            result = load_etf_universe(DummySession())

        self.assertEqual(
            list(result["etf_name"]),
            ["ACE 바이오TOP10액티브"],
        )
        self.assertEqual(result.iloc[0]["short_code"], "999999")
        self.assertEqual(result.iloc[0]["fund_code"], "KR7999999006")
        self.assertEqual(result.iloc[0]["style"], "액티브")
        self.assertEqual(result.iloc[0]["top_1"], "알테오젠")
        self.assertEqual(result.iloc[0]["top_1_weight_pct"], 11.5)
        self.assertEqual(result.iloc[0]["manager"], "한국투자신탁운용")


class MainPipelineTests(unittest.TestCase):
    def test_main_writes_outputs_from_funetf_sources(self):
        summary = pd.DataFrame(
            [
                {
                    "manager": "한국투자신탁운용",
                    "etf_name": "테스트 ETF",
                    "short_code": "123456",
                    "fund_code": "FUND123",
                    "detail_url": "",
                    "source": "FunETF",
                    "asset_class": "주식",
                    "style": "액티브",
                    "theme": "바이오",
                    "category_tags": "주식 | 액티브 | 바이오",
                    "aum_okr": 123.45,
                    "aum_unit": "억원",
                    "asof_date": "2026-03-27",
                }
            ]
        )
        official_holdings = pd.DataFrame(
            [
                {
                    "manager": "한국투자신탁운용",
                    "etf_name": "테스트 ETF",
                    "short_code": "123456",
                    "fund_code": "FUND123",
                    "holding_name": "알테오젠",
                    "weight_pct": 9.7,
                    "asof_date": "2026-03-27",
                    "source": "FunETF",
                }
            ]
        )

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            data_dir = tmp_path / "data"
            docs_dir = tmp_path / "docs"
            public_dir = tmp_path / "public"
            data_dir.mkdir()
            docs_dir.mkdir()
            public_dir.mkdir()

            with patch.object(output_files, "DATA_DIR", data_dir), \
                 patch.object(output_files, "DOCS_DIR", docs_dir), \
                 patch.object(output_files, "PUBLIC_DIR", public_dir), \
                 patch.object(pipeline, "load_etf_universe", return_value=summary), \
                 patch.object(pipeline, "resolve_item_id", return_value=("ITEM123", "https://example.com/funetf")), \
                 patch.object(
                     pipeline,
                     "fetch_top10_holdings",
                     return_value=[
                         {"citmNm": "알테오젠", "evP": "9.7"},
                     ],
                 ):
                main.main()

            output = pd.read_csv(data_dir / "etf_list.csv")
            self.assertIn("holding_count", output.columns)
            self.assertIn("holdings_source", output.columns)
            self.assertEqual(output.loc[0, "etf_name"], "테스트 ETF")
            self.assertEqual(output.loc[0, "holdings_source"], "FunETF")
            summary = pd.read_json(data_dir / "run_summary.json", typ="series")
            self.assertEqual(int(summary["etf_count"]), 1)
            self.assertEqual(int(summary["holding_count"]), 1)

    def test_main_uses_funetf_for_non_supported_managers(self):
        summary = pd.DataFrame(
            [
                {
                    "manager": "한국투자신탁운용",
                    "etf_name": "ACE 바이오TOP10액티브",
                    "short_code": "999999",
                    "fund_code": "FUND999",
                    "detail_url": "",
                    "source": "FunETF",
                    "asset_class": "주식",
                    "style": "액티브",
                    "theme": "바이오",
                    "category_tags": "주식 | 액티브 | 바이오",
                    "aum_okr": 456.78,
                    "aum_unit": "억원",
                    "asof_date": "2026-03-27",
                }
            ]
        )

        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            data_dir = tmp_path / "data"
            docs_dir = tmp_path / "docs"
            public_dir = tmp_path / "public"
            data_dir.mkdir()
            docs_dir.mkdir()
            public_dir.mkdir()

            with patch.object(output_files, "DATA_DIR", data_dir), \
                 patch.object(output_files, "DOCS_DIR", docs_dir), \
                 patch.object(output_files, "PUBLIC_DIR", public_dir), \
                 patch.object(pipeline, "load_etf_universe", return_value=summary), \
                 patch.object(pipeline, "resolve_item_id", return_value=("ITEM999", "https://example.com/funetf")), \
                 patch.object(
                     pipeline,
                     "fetch_top10_holdings",
                     return_value=[
                         {"citmNm": "알테오젠", "evP": "12.3"},
                         {"citmNm": "리가켐바이오", "evP": "8.4"},
                     ],
                 ):
                main.main()

            output = pd.read_csv(data_dir / "etf_list.csv")
            self.assertEqual(output.loc[0, "holdings_source"], "FunETF")
            holdings = pd.read_csv(data_dir / "etf_holdings.csv")
            self.assertEqual(list(holdings["holding_name"]), ["알테오젠", "리가켐바이오"])


if __name__ == "__main__":
    unittest.main()
