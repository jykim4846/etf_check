from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd


def build_html(etf_df: pd.DataFrame, holdings_df: pd.DataFrame, run_summary: dict | None = None) -> str:
    etf_json = json.dumps(etf_df.fillna("").to_dict(orient="records"), ensure_ascii=False)
    holdings_json = json.dumps(
        holdings_df.fillna("").to_dict(orient="records"), ensure_ascii=False
    )
    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    run_summary = run_summary or {}
    error_etf_count = int(run_summary.get("error_etf_count", 0) or 0)
    holding_count = int(run_summary.get("holding_count", len(holdings_df.index)) or 0)
    etf_count = int(run_summary.get("etf_count", len(etf_df.index)) or 0)
    status_label = "정상" if error_etf_count == 0 else "부분 실패"
    status_color = "#0f9d58" if error_etf_count == 0 else "#d93025"
    top_errors = run_summary.get("top_errors") or []
    top_error_html = "".join(
        f"<li>{item.get('message', 'unknown')} ({item.get('count', 0)})</li>"
        for item in top_errors[:3]
    ) or "<li>대표 오류 없음</li>"
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ETF Check</title>
  <style>
    :root {{
      --bg: #f3efe6;
      --bg-accent: #e4efe7;
      --ink: #18211b;
      --muted: #5a685f;
      --card: rgba(255,255,255,.88);
      --line: rgba(24, 33, 27, .09);
      --pill: #eef3ec;
      --shadow: 0 20px 45px rgba(46, 58, 51, .08);
      --brand: #1f6a45;
      --warn: #b64f2a;
      --status: {status_color};
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: "SF Pro Display", "Pretendard Variable", "Apple SD Gothic Neo", -apple-system, BlinkMacSystemFont, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(31,106,69,.14), transparent 28%),
        radial-gradient(circle at top right, rgba(223,153,69,.16), transparent 30%),
        linear-gradient(180deg, var(--bg) 0%, #f8f6f1 58%, #fbfaf7 100%);
    }}
    .wrap {{ max-width: 1180px; margin: 0 auto; padding: 16px 14px 40px; }}
    .card {{
      background: var(--card);
      backdrop-filter: blur(14px);
      border: 1px solid rgba(255,255,255,.7);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--shadow);
      margin-bottom: 16px;
    }}
    .hero {{
      background:
        linear-gradient(135deg, rgba(18,39,28,.96), rgba(31,106,69,.92)),
        linear-gradient(180deg, rgba(255,255,255,.08), transparent);
      color: #f6f7f2;
      border: none;
      overflow: hidden;
      position: relative;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -40px -55px auto;
      width: 180px;
      height: 180px;
      background: radial-gradient(circle, rgba(255,255,255,.16), transparent 65%);
      pointer-events: none;
    }}
    h1 {{ margin: 0; font-size: clamp(28px, 7vw, 42px); line-height: 1.02; letter-spacing: -.04em; }}
    h2 {{ margin: 0 0 8px; font-size: 20px; letter-spacing: -.03em; }}
    .eyebrow {{ font-size: 11px; font-weight: 700; letter-spacing: .18em; text-transform: uppercase; opacity: .72; }}
    .hero-copy {{ max-width: 720px; margin-top: 12px; font-size: 14px; line-height: 1.55; color: rgba(246,247,242,.8); }}
    .muted {{ color: var(--muted); font-size: 13px; line-height: 1.5; }}
    .hero .muted {{ color: rgba(246,247,242,.74); }}
    .status-row {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-top: 16px; }}
    .status-badge {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 999px;
      padding: 10px 14px;
      font-weight: 700;
      color: white;
      background: var(--status);
      box-shadow: inset 0 0 0 1px rgba(255,255,255,.12);
    }}
    .status-badge::before {{
      content: "";
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: rgba(255,255,255,.9);
    }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 18px;
    }}
    .summary-item {{
      border-radius: 18px;
      padding: 14px;
      background: rgba(255,255,255,.14);
      border: 1px solid rgba(255,255,255,.14);
    }}
    .summary-item strong {{ display: block; font-size: 22px; margin-top: 6px; letter-spacing: -.04em; }}
    .summary-item .small {{ color: rgba(246,247,242,.72); }}
    .hero-note {{
      margin-top: 14px;
      padding-top: 14px;
      border-top: 1px solid rgba(255,255,255,.12);
    }}
    .error-list {{ margin: 8px 0 0; padding-left: 18px; color: rgba(246,247,242,.85); }}
    .panel-head {{ display: flex; justify-content: space-between; gap: 12px; align-items: end; margin-bottom: 12px; }}
    .panel-kicker {{ font-size: 12px; font-weight: 700; color: var(--brand); letter-spacing: .1em; text-transform: uppercase; }}
    .nav-tabs {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-top: 16px;
    }}
    button {{
      border: 1px solid var(--line);
      background: rgba(255,255,255,.9);
      border-radius: 999px;
      padding: 11px 14px;
      cursor: pointer;
      font: inherit;
      color: var(--ink);
    }}
    button.active {{
      background: var(--ink);
      color: white;
      border-color: var(--ink);
      box-shadow: inset 0 0 0 1px rgba(255,255,255,.06);
    }}
    .filter-block {{ margin-top: 12px; }}
    .filter-title {{ font-size: 12px; color: var(--muted); margin-bottom: 8px; font-weight: 700; }}
    .filters {{
      display: flex;
      gap: 8px;
      flex-wrap: nowrap;
      overflow-x: auto;
      padding-bottom: 4px;
      scrollbar-width: none;
    }}
    .filters::-webkit-scrollbar {{ display: none; }}
    .filters button {{ flex: 0 0 auto; background: var(--pill); }}
    a {{ color: inherit; }}
    .view-section {{ display: none; }}
    .view-section.active {{ display: block; }}
    .bio-grid {{ display: grid; gap: 16px; }}
    .bio-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      background:
        linear-gradient(180deg, rgba(255,255,255,.94), rgba(250,248,243,.96));
    }}
    .bio-card h3 {{ margin: 0 0 10px; font-size: 22px; letter-spacing: -.03em; }}
    .bio-card .meta {{ color: var(--muted); font-size: 12px; margin-bottom: 12px; line-height: 1.55; }}
    .table-shell {{ overflow-x: auto; border-radius: 16px; border: 1px solid var(--line); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; background: rgba(255,255,255,.65); }}
    th, td {{ border-bottom: 1px solid var(--line); text-align: left; padding: 12px 10px; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: rgba(248,246,241,.98); z-index: 1; }}
    .num {{ text-align: right; white-space: nowrap; }}
    .small {{ font-size: 12px; color: var(--muted); }}
    .badge {{
      display: inline-block;
      padding: 4px 9px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      background: #e7f2e9;
      color: #215e3a;
    }}
    .badge-fallback {{ background: #ffeddc; color: #9b4d11; }}
    .layout-grid {{ display: grid; gap: 16px; }}
    .mobile-stack {{ display: grid; gap: 12px; }}
    .etf-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 15px;
      background: linear-gradient(180deg, rgba(255,255,255,.96), rgba(248,245,238,.96));
      box-shadow: 0 10px 25px rgba(37, 49, 42, .04);
    }}
    .etf-card.selected {{ border-color: rgba(31,106,69,.55); box-shadow: 0 14px 30px rgba(31,106,69,.12); }}
    .etf-top {{ display: flex; justify-content: space-between; gap: 12px; align-items: start; }}
    .etf-name {{ margin-top: 6px; font-size: 18px; line-height: 1.22; letter-spacing: -.03em; }}
    .etf-sub {{ margin-top: 6px; display: flex; gap: 6px; flex-wrap: wrap; }}
    .chip {{
      display: inline-flex;
      align-items: center;
      min-height: 28px;
      padding: 0 10px;
      border-radius: 999px;
      background: var(--pill);
      font-size: 12px;
      color: #314038;
      white-space: nowrap;
    }}
    .etf-metrics {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    .metric {{
      padding: 11px 12px;
      border-radius: 14px;
      background: rgba(241,238,229,.86);
      border: 1px solid rgba(24,33,27,.06);
    }}
    .metric strong {{ display: block; margin-top: 4px; font-size: 17px; letter-spacing: -.03em; }}
    .actions-row {{ display: flex; justify-content: space-between; gap: 10px; align-items: center; margin-top: 14px; }}
    .select-hint {{ font-size: 12px; color: var(--muted); }}
    .detail-link {{ font-weight: 700; color: var(--brand); text-decoration: none; }}
    .detail-link:hover {{ text-decoration: underline; }}
    .holdings-grid {{ display: grid; gap: 10px; margin-top: 14px; }}
    .holding-card {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 10px;
      align-items: center;
      padding: 12px 14px;
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,.96), rgba(247,244,237,.95));
      border: 1px solid var(--line);
    }}
    .holding-rank {{
      width: 28px;
      height: 28px;
      border-radius: 999px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      font-size: 12px;
      font-weight: 800;
      color: white;
      background: linear-gradient(135deg, #284f3a, #4a8660);
      margin-right: 10px;
      flex: 0 0 auto;
    }}
    .holding-left {{ display: flex; align-items: center; min-width: 0; }}
    .holding-name {{ font-size: 15px; line-height: 1.35; }}
    .holding-weight {{ font-size: 18px; font-weight: 800; letter-spacing: -.03em; }}
    .desktop-only {{ display: none; }}
    .section-copy {{ margin-top: 2px; }}
    @media (min-width: 820px) {{
      .wrap {{ padding: 24px 20px 56px; }}
      .card {{ padding: 22px; }}
      .summary-grid {{ grid-template-columns: repeat(4, minmax(0, 1fr)); }}
      .layout-grid {{ grid-template-columns: minmax(0, 1.3fr) minmax(360px, .8fr); }}
      .desktop-only {{ display: block; }}
      .mobile-stack {{ display: none; }}
      .nav-tabs {{ display: inline-flex; grid-template-columns: none; }}
      .filters {{ flex-wrap: wrap; overflow: visible; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card hero">
      <div class="eyebrow">Active ETF Monitor</div>
      <h1>한 화면에서 바로 보는<br />국내 액티브 ETF</h1>
      <div class="hero-copy">삼성(KODEX), 미래에셋(TIGER), 타임폴리오(TIME) 액티브 ETF를 순자산총액 기준으로 정렬했습니다. 아이폰 화면에서도 종목 선택, 보유비중 확인, 오류 상태 파악이 한 번에 되도록 재구성했습니다. 업데이트: {updated_at}</div>
      <div class="status-row">
        <span class="status-badge">최근 수집 상태: {status_label}</span>
        <a href="https://github.com/jykim4846/etf_check/actions/workflows/daily.yml">
          <img alt="Daily ETF Update status" src="https://github.com/jykim4846/etf_check/actions/workflows/daily.yml/badge.svg" />
        </a>
      </div>
      <div class="summary-grid">
        <div class="summary-item"><div class="small">ETF 수</div><strong>{etf_count}</strong></div>
        <div class="summary-item"><div class="small">구성종목 행 수</div><strong>{holding_count}</strong></div>
        <div class="summary-item"><div class="small">오류 ETF 수</div><strong>{error_etf_count}</strong></div>
        <div class="summary-item"><div class="small">생성 시각</div><strong style="font-size:14px">{run_summary.get("run_date_kst", "-")}</strong></div>
      </div>
      <div class="hero-note">
        <div class="small">대표 오류</div>
        <ul class="error-list">
          {top_error_html}
        </ul>
      </div>
      <div class="nav-tabs">
        <button id="show-bio-view" class="active">바이오 통합 보기</button>
        <button id="show-list-view">전체 ETF 보기</button>
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
        <div class="muted section-copy">세 운용사의 바이오 테마 ETF 보유종목과 비중을 한 번에 비교합니다. 모바일에서는 가장 중요한 상위 종목이 먼저 보이도록 정리했습니다.</div>
        <div id="bio-summary" class="bio-grid" style="margin-top:16px;"></div>
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
            <div class="small">모바일에서는 카드 선택, 데스크톱에서는 표 정렬</div>
          </div>
          <div id="etf-cards" class="mobile-stack"></div>
          <div class="table-shell desktop-only">
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
          <div class="table-shell desktop-only" style="margin-top:14px;">
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
      </div>
    </div>
  </div>
<script>
const etfs = {etf_json};
const holdings = {holdings_json};
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

function renderBioSummary() {{
  const container = document.getElementById('bio-summary');
  const managers = ['삼성', '미래에셋', '타임폴리오'];
  const bioEtfs = etfs.filter(x => x.theme === '바이오');
  const topByManager = {{}};
  managers.forEach(manager => {{
    const managerEtfs = bioEtfs.filter(x => x.manager === manager);
    const fundCodes = new Set(managerEtfs.map(x => x.fund_code));
    const rows = holdings
      .filter(x => fundCodes.has(x.fund_code))
      .sort((a, b) => Number(b.weight_pct || -1) - Number(a.weight_pct || -1))
      .slice(0, 10);
    topByManager[manager] = {{
      etfNames: managerEtfs.map(x => x.etf_name).join(' / '),
      sourceSet: [...new Set(managerEtfs.map(x => x.holdings_source || x.source))].join(', '),
      rows,
    }};
  }});

  const scoreMap = new Map();
  managers.forEach(manager => {{
    topByManager[manager].rows.forEach(row => {{
      const prev = scoreMap.get(row.holding_name) || 0;
      scoreMap.set(row.holding_name, Math.max(prev, Number(row.weight_pct || 0)));
    }});
  }});

  const topHoldings = [...scoreMap.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(item => item[0]);

  const metaHtml = managers.map(manager => {{
    const meta = topByManager[manager];
    return `<div><strong>${{manager}}</strong>: ${{meta.etfNames || '바이오 ETF 없음'}} / 소스: ${{meta.sourceSet || '-'}}<\/div>`;
  }}).join('');

  const bodyRows = topHoldings.map(name => {{
    const cols = managers.map(manager => {{
      const row = topByManager[manager].rows.find(item => item.holding_name === name);
      return `<td class="num">${{row ? formatNum(row.weight_pct) : '-'}}<\/td>`;
    }}).join('');
    return `<tr><td>${{name}}<\/td>${{cols}}<\/tr>`;
  }}).join('');

  container.innerHTML = `
    <div class="bio-card">
      <h3>바이오 보유종목 비교 Top 10</h3>
      <div class="meta">${{metaHtml}}<\/div>
      <table>
        <thead>
          <tr>
            <th>종목명</th>
            <th class="num">삼성</th>
            <th class="num">미래에셋</th>
            <th class="num">타임폴리오</th>
          </tr>
        </thead>
        <tbody>${{bodyRows || '<tr><td colspan="4">데이터 없음<\/td><\/tr>'}}<\/tbody>
      </table>
    </div>
  `;
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
  const cards = document.getElementById('etf-cards');
  tbody.innerHTML = '';
  cards.innerHTML = '';
  filteredEtfs().forEach(row => {{
    const tr = document.createElement('tr');
    tr.style.cursor = 'pointer';
    tr.onclick = () => {{ currentFundCode = row.fund_code; renderEtfs(); renderHoldings(); }};
    if (row.fund_code === currentFundCode) tr.style.background = '#fafafa';
    const sourceBadge = row.holdings_source === 'FunETF'
      ? '<span class="badge badge-fallback">fallback</span>'
      : `<span class="badge">${{row.holdings_source || '-'}}</span>`;
    tr.innerHTML = `
      <td>${{row.manager}}</td>
      <td><strong>${{row.etf_name}}</strong><div class="small">${{row.short_code}} · ${{sourceBadge}}</div></td>
      <td><div>${{row.asset_class || '-'}}</div><div class="small">${{row.style || '-'}} / ${{row.theme || '-'}}</div></td>
      <td>${{formatNum(row.holding_count)}}개</td>
      <td class="num">${{formatNum(row.aum_okr)}}</td>
      <td>${{row.asof_date || '-'}}</td>
      <td>${{row.detail_url ? '<a href="' + row.detail_url + '" target="_blank" rel="noreferrer">원본</a>' : '-'}}</td>`;
    tbody.appendChild(tr);

    const card = document.createElement('button');
    card.type = 'button';
    card.className = `etf-card${{row.fund_code === currentFundCode ? ' selected' : ''}}`;
    const tags = [row.asset_class, row.style, row.theme].filter(Boolean).map(tag => `<span class="chip">${{tag}}<\/span>`).join('');
    card.innerHTML = `
      <div class="etf-top">
        <div>
          <div class="small">${{row.manager}}</div>
          <div class="etf-name">${{row.etf_name}}<\/div>
        </div>
        ${{row.holdings_source === 'FunETF'
          ? '<span class="badge badge-fallback">fallback<\/span>'
          : `<span class="badge">${{row.holdings_source || '-'}}<\/span>`}}
      <\/div>
      <div class="small" style="margin-top:6px;">${{row.short_code}} · 기준일 ${{row.asof_date || '-'}}<\/div>
      <div class="etf-sub">${{tags || '<span class="chip">분류 없음<\/span>'}}<\/div>
      <div class="etf-metrics">
        <div class="metric"><div class="small">AUM(억원)<\/div><strong>${{formatNum(row.aum_okr)}}<\/strong><\/div>
        <div class="metric"><div class="small">보유종목 수<\/div><strong>${{formatNum(row.holding_count)}}개<\/strong><\/div>
      <\/div>
      <div class="actions-row">
        <div class="select-hint">${{row.error ? '오류 있음' : '탭해서 보유종목 보기'}}<\/div>
        ${{row.detail_url ? '<span class="detail-link">원본 보기<\/span>' : '<span class="small">원본 없음<\/span>'}}
      <\/div>`;
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
  const selected = etfs.find(x => x.fund_code === currentFundCode);
  document.getElementById('selected-name').textContent = selected ? `${{selected.etf_name}} / 기준일: ${{selected.asof_date || '-'}} / 카테고리: ${{selected.category_tags || '-'}} / 보유종목소스: ${{selected.holdings_source || '-'}} / 개수: ${{selected.holding_count || 0}}` : '선택된 ETF가 없습니다.';
  const rows = holdings.filter(x => x.fund_code === currentFundCode).sort((a,b) => Number(b.weight_pct || -1) - Number(a.weight_pct || -1));
  if (!rows.length) {{
    cards.innerHTML = '<div class="holding-card"><div class="holding-name">보유종목 데이터가 없습니다.<\/div><div class="small">-<\/div><\/div>';
  }}
  rows.forEach((row, index) => {{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${{row.holding_name}}</td><td class="num">${{formatNum(row.weight_pct)}}</td>`;
    tbody.appendChild(tr);

    const card = document.createElement('div');
    card.className = 'holding-card';
    card.innerHTML = `
      <div class="holding-left">
        <span class="holding-rank">${{index + 1}}<\/span>
        <div class="holding-name">${{row.holding_name}}<\/div>
      <\/div>
      <div class="holding-weight">${{formatNum(row.weight_pct)}}%<\/div>`;
    cards.appendChild(card);
  }});
}}

renderFilters();
renderEtfs();
renderHoldings();
renderBioSummary();
document.getElementById('show-bio-view').addEventListener('click', () => switchView('bio'));
document.getElementById('show-list-view').addEventListener('click', () => switchView('list'));
</script>
</body>
</html>
"""
