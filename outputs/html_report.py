from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
from config import KST


def build_html(
    etf_df: pd.DataFrame,
    holdings_df: pd.DataFrame,
    run_summary: dict | None = None,
    holding_changes_df: pd.DataFrame | None = None,
) -> str:
    etf_json = json.dumps(etf_df.fillna("").to_dict(orient="records"), ensure_ascii=False)
    holdings_json = json.dumps(holdings_df.fillna("").to_dict(orient="records"), ensure_ascii=False)
    holding_changes_json = json.dumps(
        (holding_changes_df if holding_changes_df is not None else holdings_df).fillna("").to_dict(orient="records"),
        ensure_ascii=False,
    )
    run_summary = run_summary or {}
    updated_at = run_summary.get("run_date_kst") or datetime.now(timezone.utc).astimezone(KST).strftime("%Y-%m-%d %H:%M KST")
    etf_count = int(run_summary.get("etf_count", len(etf_df.index)) or 0)
    holding_count = int(run_summary.get("holding_count", len(holdings_df.index)) or 0)
    run_date = run_summary.get("run_date_kst", "-")
    previous_run_date = run_summary.get("previous_run_date_kst") or "이전 비교 없음"
    changed_holding_count = int(run_summary.get("changed_holding_count", 0) or 0)
    new_holding_count = int(run_summary.get("new_holding_count", 0) or 0)
    removed_holding_count = int(run_summary.get("removed_holding_count", 0) or 0)
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ETF Check</title>
  <style>
    :root {{
      --bg: #f6f3ea;
      --ink: #152018;
      --muted: #647066;
      --card: rgba(255,255,255,.92);
      --line: rgba(21,32,24,.08);
      --brand: #1e6945;
      --pill: #ebf3ec;
      --shadow: 0 18px 40px rgba(33, 48, 38, .08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "SF Pro Display", "Pretendard Variable", "Apple SD Gothic Neo", -apple-system, BlinkMacSystemFont, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(30,105,69,.12), transparent 26%),
        radial-gradient(circle at top right, rgba(210,160,87,.15), transparent 28%),
        linear-gradient(180deg, var(--bg), #fbfaf6);
    }}
    a {{ color: inherit; }}
    button {{ font: inherit; }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 16px 14px 36px; }}
    .card {{
      margin-bottom: 16px;
      padding: 18px;
      border-radius: 22px;
      background: var(--card);
      border: 1px solid rgba(255,255,255,.76);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }}
    .hero {{
      color: #f5f7f1;
      background: linear-gradient(135deg, rgba(16,38,27,.97), rgba(30,105,69,.93));
      border: none;
    }}
    .eyebrow {{ font-size: 11px; letter-spacing: .18em; text-transform: uppercase; opacity: .74; font-weight: 700; }}
    h1 {{ margin: 10px 0 0; font-size: clamp(30px, 8vw, 44px); line-height: 1.02; letter-spacing: -.05em; }}
    h2 {{ margin: 0; font-size: 21px; letter-spacing: -.03em; }}
    .hero-copy {{ margin-top: 12px; max-width: 720px; font-size: 14px; line-height: 1.55; color: rgba(245,247,241,.8); }}
    .status-row {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-top: 16px; }}
    .status-badge {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 10px 14px;
      background: rgba(255,255,255,.12);
      color: white;
      font-weight: 700;
    }}
    .action-button {{
      border: 0;
      border-radius: 999px;
      padding: 11px 16px;
      background: #fff5df;
      color: #2e2616;
      font-weight: 700;
    }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-top: 18px; }}
    .summary-item {{ border-radius: 18px; padding: 14px; background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.1); }}
    .summary-item strong {{ display: block; margin-top: 6px; font-size: 22px; letter-spacing: -.04em; }}
    .small {{ font-size: 12px; color: var(--muted); }}
    .hero .small {{ color: rgba(245,247,241,.72); }}
    .hero-status {{ margin-top: 12px; font-size: 13px; color: rgba(245,247,241,.78); min-height: 19px; }}
    .nav-tabs {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-top: 16px; }}
    .tab-btn {{
      border: 1px solid var(--line);
      background: rgba(255,255,255,.92);
      color: var(--ink);
      border-radius: 999px;
      padding: 11px 14px;
    }}
    .tab-btn.active {{ background: var(--ink); color: white; border-color: var(--ink); }}
    .view-section {{ display: none; }}
    .view-section.active {{ display: block; }}
    .panel-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: end; margin-bottom: 12px; }}
    .panel-kicker {{ color: var(--brand); font-size: 12px; letter-spacing: .1em; text-transform: uppercase; font-weight: 700; }}
    .muted {{ color: var(--muted); font-size: 13px; line-height: 1.5; }}
    .filters {{ display: flex; gap: 8px; overflow-x: auto; padding-bottom: 4px; scrollbar-width: none; }}
    .filters::-webkit-scrollbar {{ display: none; }}
    .filters button {{
      flex: 0 0 auto;
      border: 1px solid var(--line);
      background: var(--pill);
      border-radius: 999px;
      padding: 9px 12px;
      color: var(--ink);
    }}
    .filters button.active {{ background: var(--ink); color: white; border-color: var(--ink); }}
    .filter-block + .filter-block {{ margin-top: 12px; }}
    .filter-title {{ margin-bottom: 8px; font-size: 12px; color: var(--muted); font-weight: 700; }}
    .layout-grid {{ display: grid; gap: 16px; }}
    .mobile-stack {{ display: grid; gap: 12px; }}
    .etf-card {{
      width: 100%;
      text-align: left;
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 15px;
      background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(247,244,236,.96));
      color: var(--ink);
    }}
    .etf-card.selected {{ border-color: rgba(30,105,69,.48); box-shadow: 0 14px 28px rgba(30,105,69,.11); }}
    .etf-top {{ display: flex; justify-content: space-between; gap: 12px; align-items: start; }}
    .etf-name {{ margin-top: 6px; font-size: 18px; line-height: 1.22; letter-spacing: -.03em; }}
    .chip-row {{ display: flex; gap: 6px; flex-wrap: wrap; margin-top: 9px; }}
    .chip {{ display: inline-flex; align-items: center; min-height: 28px; padding: 0 10px; border-radius: 999px; background: var(--pill); font-size: 12px; color: #314038; }}
    .metric-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; margin-top: 14px; }}
    .metric {{ padding: 11px 12px; border-radius: 14px; border: 1px solid var(--line); background: rgba(240,237,228,.82); }}
    .metric strong {{ display: block; margin-top: 4px; font-size: 17px; letter-spacing: -.03em; }}
    .actions-row {{ display: flex; justify-content: space-between; gap: 10px; align-items: center; margin-top: 14px; }}
    .detail-link {{ color: var(--brand); text-decoration: none; font-weight: 700; }}
    .holdings-grid {{ display: grid; gap: 10px; margin-top: 14px; }}
    .holding-card {{ display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: center; padding: 12px 14px; border-radius: 16px; border: 1px solid var(--line); background: linear-gradient(180deg, rgba(255,255,255,.98), rgba(248,245,239,.96)); }}
    .holding-left {{ display: flex; align-items: center; min-width: 0; }}
    .holding-rank {{ width: 28px; height: 28px; border-radius: 999px; display: inline-flex; align-items: center; justify-content: center; margin-right: 10px; color: white; background: linear-gradient(135deg, #294f3b, #4b8762); font-size: 12px; font-weight: 800; }}
    .holding-name {{ font-size: 15px; line-height: 1.35; }}
    .holding-weight {{ font-size: 18px; font-weight: 800; letter-spacing: -.03em; }}
    .holding-weight-wrap {{ text-align: right; }}
    .metric-label {{ font-size: 11px; color: var(--muted); font-weight: 700; }}
    .holding-delta {{ margin-top: 4px; font-size: 12px; font-weight: 700; }}
    .delta-up {{ color: #16794f; }}
    .delta-down {{ color: #b84b3f; }}
    .delta-flat {{ color: var(--muted); }}
    .delta-new {{ color: #906717; }}
    .delta-removed {{ color: #7c3d37; }}
    .change-chip {{
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 800;
      background: #edf2eb;
      color: #314038;
    }}
    .table-shell {{ display: none; }}
    .bio-card {{ border: 1px solid var(--line); border-radius: 18px; padding: 16px; background: linear-gradient(180deg, rgba(255,255,255,.96), rgba(249,247,241,.96)); }}
    .bio-card h3 {{ margin: 0 0 10px; font-size: 22px; letter-spacing: -.03em; }}
    .bio-card .meta {{ color: var(--muted); font-size: 12px; line-height: 1.55; margin-bottom: 12px; }}
    .bio-summary-grid {{ display: grid; gap: 12px; }}
    .bio-stock-card {{ border: 1px solid var(--line); border-radius: 18px; padding: 16px; background: rgba(255,255,255,.88); }}
    .bio-stock-top {{ display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; }}
    .bio-stock-name {{ font-size: 19px; line-height: 1.2; letter-spacing: -.03em; }}
    .bio-stock-total {{ text-align: right; }}
    .bio-stock-total strong {{ display: block; font-size: 24px; letter-spacing: -.04em; }}
    .bio-fund-list {{ display: grid; gap: 8px; margin-top: 14px; }}
    .bio-fund-row {{ display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: center; border: 1px solid var(--line); border-radius: 14px; padding: 10px 12px; background: rgba(246,243,236,.88); }}
    .bio-fund-name {{ font-size: 13px; line-height: 1.35; }}
    .bio-fund-weight {{ font-size: 16px; font-weight: 800; letter-spacing: -.03em; }}
    .bio-fund-meta {{ text-align: right; }}
    .bio-fund-delta {{ margin-top: 4px; font-size: 12px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 12px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: rgba(248,246,241,.98); }}
    .num {{ text-align: right; white-space: nowrap; }}
    @media (min-width: 820px) {{
      .wrap {{ padding: 24px 20px 56px; }}
      .card {{ padding: 22px; }}
      .summary-grid {{ grid-template-columns: repeat(4, minmax(0, 1fr)); }}
      .layout-grid {{ grid-template-columns: minmax(0, 1.25fr) minmax(360px, .8fr); }}
      .table-shell {{ display: block; overflow-x: auto; border: 1px solid var(--line); border-radius: 16px; }}
      .mobile-stack {{ display: none; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card hero">
      <div class="eyebrow">Active ETF Monitor</div>
      <h1>국내 액티브 ETF<br />수집 결과</h1>
      <div class="hero-copy">이 페이지는 마지막 성공 수집 결과를 보여줍니다. 수집 실패는 숨기지 않고 그대로 실패로 처리합니다. 업데이트: {updated_at}</div>
      <div class="status-row">
        <span class="status-badge">최근 성공 수집 기준 표시</span>
        <button id="manual-collect-button" class="action-button" type="button">수동 재수집 실행</button>
      </div>
      <div id="manual-collect-status" class="hero-status"></div>
      <div class="summary-grid">
        <div class="summary-item"><div class="small">ETF 수</div><strong>{etf_count}</strong></div>
        <div class="summary-item"><div class="small">구성종목 행 수</div><strong>{holding_count}</strong></div>
        <div class="summary-item"><div class="small">직전 대비 변동</div><strong>{changed_holding_count}</strong><div class="small">신규 {new_holding_count} / 제외 {removed_holding_count}</div></div>
        <div class="summary-item"><div class="small">직전 수집 시각</div><strong style="font-size:14px">{previous_run_date}</strong><div class="small">최근 생성 {run_date}</div></div>
      </div>
      <div class="nav-tabs">
        <button id="show-bio-view" class="tab-btn active" type="button">바이오 통합 보기</button>
        <button id="show-list-view" class="tab-btn" type="button">전체 ETF 보기</button>
      </div>
    </div>
    <div id="bio-view" class="view-section active">
      <div class="card">
        <div class="panel-head">
          <div>
            <div class="panel-kicker">Landing View</div>
            <h2>바이오 섹터 통합 요약</h2>
          </div>
        </div>
        <div class="muted">바이오 테마 ETF에 편입된 종목을 종목 기준으로 다시 묶었습니다. 각 종목이 어떤 펀드에 얼마나 들어있는지 바로 볼 수 있습니다.</div>
        <div id="bio-summary" style="margin-top:16px;"></div>
      </div>
    </div>
    <div id="list-view" class="view-section">
      <div class="card">
        <div class="filter-block" id="manager-filters"></div>
        <div class="filter-block" id="asset-filters"></div>
        <div class="filter-block" id="theme-filters"></div>
      </div>
      <div class="layout-grid">
        <div class="card">
          <div class="panel-head">
            <div>
              <div class="panel-kicker">Universe</div>
              <h2>ETF 목록</h2>
            </div>
          </div>
          <div id="etf-cards" class="mobile-stack"></div>
          <div class="table-shell">
            <table id="etf-table">
              <thead>
                <tr>
                  <th>운용사</th>
                  <th>ETF</th>
                  <th>카테고리</th>
                  <th>보유종목</th>
                  <th class="num">AUM(억원)</th>
                  <th>기준일</th>
                  <th>상세</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="panel-head">
            <div>
              <div class="panel-kicker">Holdings</div>
              <h2>구성종목</h2>
            </div>
          </div>
          <div class="small" id="selected-name">ETF를 선택하면 구성종목이 표시됩니다.</div>
          <div id="holdings-cards" class="holdings-grid"></div>
          <div class="table-shell" style="margin-top:14px;">
            <table id="holdings-table">
              <thead>
                <tr>
                  <th>종목명</th>
                  <th class="num">현재 비중 / 변동</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
  <script>
const etfs = {etf_json};
const holdings = {holdings_json};
const holdingChanges = {holding_changes_json};
let currentManager = '전체';
let currentAssetClass = '전체';
let currentTheme = '전체';
let currentFundCode = etfs.length ? etfs[0].fund_code : null;

function switchView(nextView) {{
  document.getElementById('bio-view').classList.toggle('active', nextView === 'bio');
  document.getElementById('list-view').classList.toggle('active', nextView === 'list');
  document.getElementById('show-bio-view').classList.toggle('active', nextView === 'bio');
  document.getElementById('show-list-view').classList.toggle('active', nextView === 'list');
}}

function formatNum(value) {{
  if (value === '' || value === null || value === undefined) return '-';
  const num = Number(value);
  if (Number.isNaN(num)) return String(value);
  return num.toLocaleString('ko-KR', {{ maximumFractionDigits: 2 }});
}}

function formatDateTimeKst(value) {{
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat('ko-KR', {{
    timeZone: 'Asia/Seoul',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }}).format(date);
}}

function uniqueOptions(key) {{
  return ['전체', ...new Set(etfs.map(item => item[key]).filter(Boolean))];
}}

function diffClass(changeState, diffValue) {{
  if (changeState === '신규' || changeState === '첫수집') return 'delta-new';
  if (changeState === '제외') return 'delta-removed';
  if (Number(diffValue) > 0.0001) return 'delta-up';
  if (Number(diffValue) < -0.0001) return 'delta-down';
  return 'delta-flat';
}}

function diffLabel(changeState, diffValue, previousValue) {{
  if (changeState === '첫수집') return '첫 수집';
  if (changeState === '신규') return '변동 신규 편입';
  if (changeState === '제외') return '변동 제외 ' + formatNum(Math.abs(Number(previousValue || 0))) + '%';
  const diff = Number(diffValue || 0);
  if (Math.abs(diff) <= 0.0001) return '변동 없음';
  const prefix = diff > 0 ? '+' : '';
  return '변동 ' + prefix + formatNum(diff) + '%p';
}}

function filteredEtfs() {{
  return [...etfs.filter(item => {{
    if (currentManager !== '전체' && item.manager !== currentManager) return false;
    if (currentAssetClass !== '전체' && item.asset_class !== currentAssetClass) return false;
    if (currentTheme !== '전체' && item.theme !== currentTheme) return false;
    return true;
  }})].sort((a, b) => Number(b.aum_okr || -1) - Number(a.aum_okr || -1));
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
  options.forEach(option => {{
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.textContent = option;
    if (option === currentValue) btn.classList.add('active');
    btn.onclick = () => onSelect(option);
    wrap.appendChild(btn);
  }});
  root.appendChild(wrap);
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

function renderBioSummary() {{
  const container = document.getElementById('bio-summary');
  const bioEtfs = etfs.filter(item => item.theme === '바이오');
  if (!bioEtfs.length) {{
    container.innerHTML = '<div class="bio-card"><h3>바이오 종목 노출 현황</h3><div class="meta">바이오 테마 ETF 데이터가 없습니다.<\\/div><\\/div>';
    return;
  }}

  const bioFundMap = new Map(bioEtfs.map(item => [item.fund_code, item]));
  const grouped = new Map();
  holdings
    .filter(item => bioFundMap.has(item.fund_code))
    .forEach(item => {{
      const stockName = item.holding_name;
      const fund = bioFundMap.get(item.fund_code);
      const currentWeight = Number(item.current_weight_pct ?? item.weight_pct ?? 0);
      const compareRow = holdingChanges.find(change =>
        change.fund_code === item.fund_code && change.holding_name === item.holding_name
      ) || {{}};
      const current = grouped.get(stockName) || {{
        holding_name: stockName,
        total_weight: 0,
        funds: [],
      }};
      current.total_weight += currentWeight;
      current.funds.push({{
        etf_name: fund.etf_name,
        manager: fund.manager,
        weight_pct: currentWeight,
        previous_weight_pct: Number(compareRow.previous_weight_pct || 0),
        weight_diff_pct: Number(compareRow.weight_diff_pct || 0),
        change_state: compareRow.change_state || '유지',
      }});
      grouped.set(stockName, current);
    }});

  const rows = [...grouped.values()]
    .map(item => ({{
      ...item,
      fund_count: item.funds.length,
      funds: [...item.funds].sort((a, b) => b.weight_pct - a.weight_pct),
    }}))
    .sort((a, b) => b.total_weight - a.total_weight);

  const cardsHtml = rows.map((row, index) => {{
    const fundsHtml = row.funds.map(fund => `
      <div class="bio-fund-row">
        <div class="bio-fund-name">${{fund.manager}} · ${{fund.etf_name}}<\\/div>
        <div class="bio-fund-meta">
          <div class="metric-label">현재 비중<\\/div>
          <div class="bio-fund-weight">${{formatNum(fund.weight_pct)}}%<\\/div>
          <div class="bio-fund-delta ${{diffClass(fund.change_state, fund.weight_diff_pct)}}">${{diffLabel(fund.change_state, fund.weight_diff_pct, fund.previous_weight_pct)}}<\\/div>
        <\\/div>
      </div>
    `).join('');
    return `
      <div class="bio-stock-card">
        <div class="bio-stock-top">
          <div>
            <div class="small">종목 ${'{'}index + 1{'}'} · 편입 펀드 ${'{'}row.fund_count{'}'}개<\\/div>
            <div class="bio-stock-name">${'{'}row.holding_name{'}'}<\\/div>
          </div>
          <div class="bio-stock-total">
            <div class="small">합산 비중<\\/div>
            <strong>${'{'}formatNum(row.total_weight){'}'}%<\\/strong>
          </div>
        </div>
        <div class="bio-fund-list">${'{'}fundsHtml{'}'}<\\/div>
      </div>
    `;
  }}).join('');

  container.innerHTML = `
    <div class="bio-card">
      <h3>바이오 종목별 편입 비중<\\/h3>
      <div class="meta">정렬 기준은 각 종목의 합산 편입 비중입니다. 카드 안에서 어떤 펀드에 몇 %가 들어있는지, 직전 수집 대비 얼마나 늘거나 줄었는지 바로 볼 수 있습니다.<\\/div>
      <div class="bio-summary-grid">${'{'}cardsHtml || '<div class="small">데이터 없음<\\/div>'{'}'}<\\/div>
    </div>`;
}}

function rerenderAfterFilter() {{
  const rows = filteredEtfs();
  currentFundCode = rows.length ? rows[0].fund_code : null;
  renderFilters();
  renderEtfs();
  renderHoldings();
}}

function renderEtfs() {{
  const tbody = document.querySelector('#etf-table tbody');
  const cards = document.getElementById('etf-cards');
  tbody.innerHTML = '';
  cards.innerHTML = '';

  filteredEtfs().forEach(row => {{
    const tr = document.createElement('tr');
    tr.style.cursor = 'pointer';
    tr.onclick = () => {{ currentFundCode = row.fund_code; renderEtfs(); renderHoldings(); }};
    if (row.fund_code === currentFundCode) tr.style.background = '#f4f7f2';
    tr.innerHTML = `
      <td>${{row.manager}}<\\/td>
      <td><strong>${{row.etf_name}}<\\/strong><div class="small">${{row.short_code}} · ${{row.holdings_source || '-'}}<\\/div><\\/td>
      <td><div>${{row.asset_class || '-'}}<\\/div><div class="small">${{row.style || '-'}} / ${{row.theme || '-'}}<\\/div><\\/td>
      <td>${{formatNum(row.holding_count)}}개<\\/td>
      <td class="num">${{formatNum(row.aum_okr)}}<\\/td>
      <td>${{row.asof_date || '-'}}<\\/td>
      <td>${{row.detail_url ? '<a href="' + row.detail_url + '" target="_blank" rel="noreferrer">원본</a>' : '-'}}<\\/td>`;
    tbody.appendChild(tr);

    const card = document.createElement('button');
    card.type = 'button';
    card.className = `etf-card${{row.fund_code === currentFundCode ? ' selected' : ''}}`;
    const chips = [row.asset_class, row.style, row.theme].filter(Boolean).map(tag => `<span class="chip">${{tag}}<\\/span>`).join('');
    card.innerHTML = `
      <div class="etf-top">
        <div>
          <div class="small">${{row.manager}}<\\/div>
          <div class="etf-name">${{row.etf_name}}<\\/div>
        </div>
        <span class="chip">${{row.holdings_source || '-'}}<\\/span>
      </div>
      <div class="small" style="margin-top:6px;">${{row.short_code}} · 기준일 ${{row.asof_date || '-'}}<\\/div>
      <div class="chip-row">${{chips}}<\\/div>
      <div class="metric-grid">
        <div class="metric"><div class="small">AUM(억원)<\\/div><strong>${{formatNum(row.aum_okr)}}<\\/strong><\\/div>
        <div class="metric"><div class="small">보유종목 수<\\/div><strong>${{formatNum(row.holding_count)}}개<\\/strong><\\/div>
      </div>
      <div class="actions-row">
        <div class="small">탭해서 보유종목 보기<\\/div>
        ${{row.detail_url ? '<span class="detail-link">원본 보기<\\/span>' : '<span class="small">원본 없음<\\/span>'}}
      </div>`;
    card.onclick = () => {{
      currentFundCode = row.fund_code;
      renderEtfs();
      renderHoldings();
    }};
    cards.appendChild(card);
  }});
}}

function renderHoldings() {{
  const tbody = document.querySelector('#holdings-table tbody');
  const cards = document.getElementById('holdings-cards');
  tbody.innerHTML = '';
  cards.innerHTML = '';

  const selected = etfs.find(item => item.fund_code === currentFundCode);
  const selectedChanges = holdingChanges.filter(item => item.fund_code === currentFundCode);
  const changedCount = selectedChanges.filter(item => ['신규', '증가', '감소', '제외'].includes(item.change_state)).length;
  const addedCount = selectedChanges.filter(item => item.change_state === '신규').length;
  const removedCount = selectedChanges.filter(item => item.change_state === '제외').length;
  document.getElementById('selected-name').textContent = selected
    ? `${{selected.etf_name}} / 기준일: ${{selected.asof_date || '-'}} / 카테고리: ${{selected.category_tags || '-'}} / 수집소스: ${{selected.holdings_source || '-'}} / 변동 ${{changedCount}}건 / 신규 ${{addedCount}} / 제외 ${{removedCount}}`
    : '선택된 ETF가 없습니다.';

  const rows = holdingChanges
    .filter(item => item.fund_code === currentFundCode)
    .sort((a, b) => Number(b.current_weight_pct || -1) - Number(a.current_weight_pct || -1));

  if (!rows.length) {{
    cards.innerHTML = '<div class="holding-card"><div class="holding-name">보유종목 데이터가 없습니다.<\\/div><div class="small">-<\\/div><\\/div>';
  }}

  rows.forEach((row, index) => {{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${{row.holding_name}} <span class="change-chip">${{row.change_state || '유지'}}<\\/span><\\/td><td class="num"><div class="metric-label">현재 비중<\\/div><div class="holding-weight">${{formatNum(row.current_weight_pct)}}%<\\/div><div class="holding-delta ${{diffClass(row.change_state, row.weight_diff_pct)}}">${{diffLabel(row.change_state, row.weight_diff_pct, row.previous_weight_pct)}}<\\/div><\\/td>`;
    tbody.appendChild(tr);

    const card = document.createElement('div');
    card.className = 'holding-card';
    card.innerHTML = `
      <div class="holding-left">
        <span class="holding-rank">${{index + 1}}<\\/span>
        <div>
          <div class="holding-name">${{row.holding_name}}<\\/div>
          <div class="holding-delta ${{diffClass(row.change_state, row.weight_diff_pct)}}">${{diffLabel(row.change_state, row.weight_diff_pct, row.previous_weight_pct)}}<\\/div>
        <\\/div>
      </div>
      <div class="holding-weight-wrap">
        <div class="metric-label">현재 비중<\\/div>
        <div class="holding-weight">${{formatNum(row.current_weight_pct)}}%<\\/div>
        <div class="small">직전 ${{row.previous_weight_pct === '' ? '-' : formatNum(row.previous_weight_pct) + '%'}}<\\/div>
      <\\/div>`;
    cards.appendChild(card);
  }});
}}

async function triggerManualCollect() {{
  const status = document.getElementById('manual-collect-status');
  const button = document.getElementById('manual-collect-button');
  const triggerToken = window.prompt('수동 재수집 토큰을 입력하세요.');
  if (!triggerToken) {{
    status.textContent = '수동 재수집이 취소되었습니다.';
    return;
  }}

  button.disabled = true;
  status.textContent = 'GitHub Actions 재수집을 요청하는 중입니다...';
  try {{
    const response = await fetch('/api/trigger-collector', {{
      method: 'POST',
      headers: {{
        'Content-Type': 'application/json',
      }},
      body: JSON.stringify({{ triggerToken }}),
    }});
    const result = await response.json();
    if (!response.ok) {{
      throw new Error(result.error || 'dispatch failed');
    }}
    status.innerHTML = `재수집 요청이 접수되었습니다. <a href="${{result.actions_url}}" target="_blank" rel="noreferrer">Actions 열기</a>`;
    await pollCollectorStatus(result.actions_url);
  }} catch (error) {{
    status.textContent = `재수집 요청 실패: ${{error.message}}`;
  }} finally {{
    button.disabled = false;
  }}
}}

async function pollCollectorStatus(actionsUrl) {{
  const status = document.getElementById('manual-collect-status');
  for (let attempt = 0; attempt < 20; attempt += 1) {{
    try {{
      const response = await fetch('/api/collector-status');
      const result = await response.json();
      if (!response.ok) {{
        throw new Error(result.error || 'status lookup failed');
      }}
      if (!result.run) {{
        status.innerHTML = `재수집 요청은 접수됐지만 아직 새 런이 보이지 않습니다. <a href="${{actionsUrl || result.actions_url}}" target="_blank" rel="noreferrer">Actions 확인</a>`;
      }} else {{
        const run = result.run;
        const runUrl = run.html_url || actionsUrl || result.actions_url;
        if (run.status === 'completed') {{
          const label = run.conclusion === 'success'
            ? '수집 성공'
            : '수집 실패 (' + (run.conclusion || 'unknown') + ')';
          status.innerHTML = `${{label}} · 마지막 갱신 ${{formatDateTimeKst(run.updated_at)}} · <a href="${{runUrl}}" target="_blank" rel="noreferrer">런 보기</a>`;
          return;
        }}
        const runState = run.status === 'in_progress' ? '실행 중' : '대기 중';
        status.innerHTML = `${{runState}} · 시작 ${{formatDateTimeKst(run.created_at)}} · <a href="${{runUrl}}" target="_blank" rel="noreferrer">런 보기</a>`;
      }}
    }} catch (error) {{
      status.textContent = `상태 확인 실패: ${{error.message}}`;
      return;
    }}
    await new Promise(resolve => setTimeout(resolve, 4000));
  }}
  status.innerHTML = `상태 확인 시간이 초과되었습니다. <a href="${{actionsUrl}}" target="_blank" rel="noreferrer">GitHub Actions에서 직접 확인</a>`;
}}

renderFilters();
renderEtfs();
renderHoldings();
renderBioSummary();
document.getElementById('show-bio-view').addEventListener('click', () => switchView('bio'));
document.getElementById('show-list-view').addEventListener('click', () => switchView('list'));
document.getElementById('manual-collect-button').addEventListener('click', triggerManualCollect);
  </script>
</body>
</html>
"""
