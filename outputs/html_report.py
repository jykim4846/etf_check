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
    .status-row {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-top: 12px; }}
    .status-badge {{ display: inline-flex; align-items: center; border-radius: 999px; padding: 8px 12px; font-weight: 700; color: white; background: {status_color}; }}
    .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-top: 12px; }}
    .summary-item {{ border: 1px solid #eee; border-radius: 12px; padding: 12px; background: #fafafa; }}
    .summary-item strong {{ display: block; font-size: 18px; margin-top: 4px; }}
    .error-list {{ margin: 10px 0 0; padding-left: 18px; color: #444; }}
    .badge {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; background:#eef3ff; color:#234; }}
    .badge-fallback {{ background:#fff2e8; color:#a24b00; }}
    .nav-tabs {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:12px; }}
    .view-section {{ display:none; }}
    .view-section.active {{ display:block; }}
    .bio-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(300px, 1fr)); gap:16px; }}
    .bio-card {{ border:1px solid #eee; border-radius:12px; padding:14px; background:#fff; }}
    .bio-card h3 {{ margin:0 0 8px; font-size:18px; }}
    .bio-card .meta {{ color:#666; font-size:12px; margin-bottom:10px; }}
    .bio-card table {{ font-size:13px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>액티브 ETF 체크</h1>
      <div class="muted">삼성(KODEX), 미래에셋(TIGER), 타임폴리오(TIME) 액티브 ETF를 순자산총액(AUM) 기준으로 정렬합니다. 업데이트: {updated_at}</div>
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
      <div class="small" style="margin-top:12px">대표 오류</div>
      <ul class="error-list">
        {top_error_html}
      </ul>
      <div class="nav-tabs">
        <button id="show-bio-view" class="active">바이오 통합 보기</button>
        <button id="show-list-view">전체 ETF 보기</button>
      </div>
    </div>
    <div id="bio-view" class="view-section active">
      <div class="card">
        <h2 style="margin-top:0;font-size:20px;">바이오 섹터 통합 요약</h2>
        <div class="muted">기본 랜딩 뷰입니다. 세 운용사의 바이오 테마 ETF 보유종목과 비중을 한 번에 비교합니다.</div>
        <div id="bio-summary" class="bio-grid" style="margin-top:16px;"></div>
      </div>
    </div>
    <div id="list-view" class="view-section">
      <div class="card">
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
              <th>보유종목</th>
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
  container.innerHTML = managers.map(manager => {{
    const managerEtfs = bioEtfs.filter(x => x.manager === manager);
    if (!managerEtfs.length) {{
      return `<div class="bio-card"><h3>${{manager}}</h3><div class="meta">바이오 테마 ETF 없음</div></div>`;
    }}
    const fundCodes = new Set(managerEtfs.map(x => x.fund_code));
    const rows = holdings
      .filter(x => fundCodes.has(x.fund_code))
      .sort((a, b) => Number(b.weight_pct || -1) - Number(a.weight_pct || -1))
      .slice(0, 20);
    const sourceSet = [...new Set(managerEtfs.map(x => x.holdings_source || x.source))].join(', ');
    const etfNames = managerEtfs.map(x => x.etf_name).join(' / ');
    const tableRows = rows.map(row => `
      <tr>
        <td>${{row.etf_name}}</td>
        <td>${{row.holding_name}}</td>
        <td class="num">${{formatNum(row.weight_pct)}}</td>
      </tr>
    `).join('');
    return `
      <div class="bio-card">
        <h3>${{manager}}</h3>
        <div class="meta">${{etfNames}}<br>보유종목소스: ${{sourceSet}}</div>
        <table>
          <thead>
            <tr>
              <th>ETF</th>
              <th>종목명</th>
              <th class="num">비중(%)</th>
            </tr>
          </thead>
          <tbody>${{tableRows || '<tr><td colspan="3">데이터 없음</td></tr>'}}</tbody>
        </table>
      </div>
    `;
  }}).join('');
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
  }});
}}

function renderHoldings() {{
  const tbody = document.querySelector('#holdings-table tbody');
  tbody.innerHTML = '';
  const selected = etfs.find(x => x.fund_code === currentFundCode);
  document.getElementById('selected-name').textContent = selected ? `${{selected.etf_name}} / 기준일: ${{selected.asof_date || '-'}} / 카테고리: ${{selected.category_tags || '-'}} / 보유종목소스: ${{selected.holdings_source || '-'}} / 개수: ${{selected.holding_count || 0}}` : '선택된 ETF가 없습니다.';
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
renderBioSummary();
document.getElementById('show-bio-view').addEventListener('click', () => switchView('bio'));
document.getElementById('show-list-view').addEventListener('click', () => switchView('list'));
</script>
</body>
</html>
"""
