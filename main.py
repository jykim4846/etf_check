from __future__ import annotations

from config import DATA_DIR, DOCS_DIR
from pipeline import build_holdings_from_api, build_holdings_from_row, run
from outputs import build_html
from transforms import (
    build_category_tags,
    classify_asset_class,
    classify_style,
    classify_theme,
    compact_text,
    extract_asof,
    extract_aum,
    find_column,
    is_overseas_etf,
    manager_from_text,
    normalize_columns,
    parse_float,
)
from collectors import (
    fetch_bytes,
    fetch_search_form,
    fetch_text,
    fetch_top10_holdings,
    load_etf_universe,
    resolve_item_id,
    session_with_retries,
)


def main() -> None:
    run()


if __name__ == "__main__":
    main()
