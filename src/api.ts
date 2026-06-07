import express from "express";
import * as sql from "mssql";
import { Client } from "pg";
import path from "path";
import { loadProgress, loadFailed, loadMetrics } from "./store";
import { log } from "./logger";
import { DEST_CONFIG, SOURCE_CONFIG } from "./config";

const app = express();
const PORT = 3001;

app.use("/assets", express.static(path.join(__dirname, "../assets")));

app.get("/api/status", async (_req, res) => {
  const progress = loadProgress();
  const failed = loadFailed();
  const metrics = loadMetrics();

  let actualRowsInEDW: number | null = null;
  let totalSourceRows: number | null = null;

  try {
    const pool = await sql.connect(DEST_CONFIG);
    const result = await pool
      .request()
      .query("SELECT COUNT(*) AS cnt FROM dbo.factInvoice");
    actualRowsInEDW = result.recordset[0].cnt;
  } catch {
    actualRowsInEDW = null;
  }

  try {
    const source = new Client(SOURCE_CONFIG);
    await source.connect();
    const result = await source.query<{ max_id: string }>(
      "SELECT MAX(id)::text AS max_id FROM invoices WHERE deleted_at IS NULL",
    );
    totalSourceRows = parseInt(result.rows[0].max_id, 10);
    await source.end();
  } catch {
    totalSourceRows = null;
  }

  const percentComplete =
    actualRowsInEDW !== null && totalSourceRows !== null
      ? ((actualRowsInEDW / totalSourceRows) * 100).toFixed(2)
      : null;

  // Rolling average rows/sec from last 10 batches
  const recentMetrics = metrics.slice(-10);
  const avgRowsPerSecond =
    recentMetrics.length > 0
      ? Math.round(
          recentMetrics.reduce((sum, m) => sum + m.rowsPerSecond, 0) /
            recentMetrics.length,
        )
      : null;

  // ETA calculation
  let etaMs: number | null = null;
  if (
    avgRowsPerSecond &&
    avgRowsPerSecond > 0 &&
    actualRowsInEDW !== null &&
    totalSourceRows !== null
  ) {
    const remaining = totalSourceRows - actualRowsInEDW;
    const secondsLeft = remaining / avgRowsPerSecond;
    etaMs = Date.now() + secondsLeft * 1000;
  }

  res.json({
    status: "running",
    totalRowsInserted: actualRowsInEDW,
    totalSourceRows,
    percentComplete,
    totalBatchesCompleted: progress?.totalBatchesCompleted ?? 0,
    lastCompletedBatchStart: progress?.lastCompletedBatchStart ?? 0,
    failedRanges: failed.length,
    lastUpdatedAt: progress?.lastUpdatedAt ?? null,
    startedAt: progress?.startedAt ?? null,
    avgRowsPerSecond,
    etaMs,
    sparkline: recentMetrics.map((m) => ({
      rps: m.rowsPerSecond,
      completedAt: m.completedAt,
    })),
    sourceUnreachable: totalSourceRows === null,
    destUnreachable: actualRowsInEDW === null,
  });
});

app.get("/", (_req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>NRS Data Intelligence Monitor</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap" rel="stylesheet" />
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root[data-theme="dark"] {
      --red: #C0272D; --bg: #0a0a0a; --surface: #111111;
      --border: #1e1e1e; --border-bright: #2e2e2e;
      --text: #e8e8e8; --text-dim: #666; --text-muted: #444;
      --green: #22c55e; --amber: #f59e0b;
      --toggle-bg: #1e1e1e; --toggle-icon: #888;
      --pill-ok-bg: rgba(34,197,94,0.1); --pill-fail-bg: rgba(192,39,45,0.1); --pill-run-bg: rgba(245,158,11,0.1);
      --spark-bar: rgba(192,39,45,0.6); --spark-bar-hover: #C0272D;
    }

    :root[data-theme="light"] {
      --red: #C0272D; --bg: #f0f0f0; --surface: #ffffff;
      --border: #e0e0e0; --border-bright: #d0d0d0;
      --text: #111111; --text-dim: #555; --text-muted: #999;
      --green: #16a34a; --amber: #d97706;
      --toggle-bg: #e8e8e8; --toggle-icon: #555;
      --pill-ok-bg: rgba(22,163,74,0.08); --pill-fail-bg: rgba(192,39,45,0.08); --pill-run-bg: rgba(217,119,6,0.08);
      --spark-bar: rgba(192,39,45,0.4); --spark-bar-hover: #C0272D;
    }

    html, body { background: var(--bg); color: var(--text); font-family: 'IBM Plex Sans', sans-serif; min-height: 100vh; font-size: 14px; transition: background 0.2s, color 0.2s; }
    body { display: flex; flex-direction: column; }

    header { border-bottom: 1px solid var(--border); padding: 16px 40px; display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; background: var(--bg); z-index: 10; transition: background 0.2s, border-color 0.2s; }
    .logo { display: flex; align-items: center; gap: 16px; }
    .logo img { height: 56px; object-fit: contain; }
    .logo-divider { width: 1px; height: 36px; background: var(--border-bright); }
    .logo-text { font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 500; letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-muted); line-height: 1.6; }
    .logo-text strong { color: var(--text); display: block; font-size: 13px; letter-spacing: 0.04em; }

    .header-right { display: flex; align-items: center; gap: 16px; }
    .live-badge { display: flex; align-items: center; gap: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 500; letter-spacing: 0.1em; text-transform: uppercase; color: var(--green); }
    .live-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--green); animation: pulse 2s infinite; }
    @keyframes pulse { 0%,100%{opacity:1;transform:scale(1)}50%{opacity:0.4;transform:scale(0.8)} }
    .refresh-info { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--text-muted); }
    .theme-toggle { cursor: pointer; background: var(--toggle-bg); border: 1px solid var(--border); padding: 6px 10px; display: flex; align-items: center; gap: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--toggle-icon); transition: all 0.2s; user-select: none; }
    .theme-toggle:hover { border-color: var(--red); color: var(--red); }
    .theme-toggle svg { width: 14px; height: 14px; fill: currentColor; flex-shrink: 0; }

    main { flex: 1; padding: 40px; max-width: 1200px; width: 100%; margin: 0 auto; }

    /* HERO */
    .hero { background: var(--surface); border: 1px solid var(--border); padding: 40px; margin-bottom: 24px; position: relative; overflow: hidden; transition: background 0.2s, border-color 0.2s; }
    .hero::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; background: var(--red); }
    .hero-label { font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 500; letter-spacing: 0.12em; text-transform: uppercase; color: var(--text-muted); margin-bottom: 16px; }
    .hero-number { font-family: 'IBM Plex Mono', monospace; font-size: clamp(48px,8vw,96px); font-weight: 600; color: var(--red); line-height: 1; letter-spacing: -0.03em; margin-bottom: 8px; }
    .hero-sub { font-size: 13px; color: var(--text-dim); font-family: 'IBM Plex Mono', monospace; }
    .progress-section { margin-top: 32px; }
    .progress-header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 10px; }
    .progress-label { font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 500; letter-spacing: 0.1em; text-transform: uppercase; color: var(--text-dim); }
    .progress-pct { font-family: 'IBM Plex Mono', monospace; font-size: 28px; font-weight: 600; color: var(--red); line-height: 1; }
    .progress-track { height: 4px; background: var(--border-bright); position: relative; overflow: hidden; }
    .progress-fill { height: 100%; background: var(--red); transition: width 1.2s ease; position: relative; }
    .progress-fill::after { content: ''; position: absolute; right: 0; top: 0; bottom: 0; width: 40px; background: linear-gradient(to right,transparent,rgba(255,255,255,0.3)); animation: shimmer 2s infinite; }
    @keyframes shimmer { 0%{opacity:0}50%{opacity:1}100%{opacity:0} }
    .progress-ends { display: flex; justify-content: space-between; margin-top: 8px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--text-muted); }

    /* GRID */
    .grid { display: grid; grid-template-columns: repeat(4,1fr); gap: 16px; margin-bottom: 24px; }
    .card { background: var(--surface); border: 1px solid var(--border); padding: 24px; transition: background 0.2s, border-color 0.2s; }
    .card-label { font-family: 'IBM Plex Mono', monospace; font-size: 10px; font-weight: 500; letter-spacing: 0.12em; text-transform: uppercase; color: var(--text-muted); margin-bottom: 12px; }
    .card-value { font-family: 'IBM Plex Mono', monospace; font-size: 24px; font-weight: 600; color: var(--text); line-height: 1; }
    .card-value.green { color: var(--green); } .card-value.amber { color: var(--amber); } .card-value.red { color: var(--red); }
    .card-sub { margin-top: 6px; font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--text-muted); }

    /* SPARKLINE + TABLE ROW */
    .bottom-grid { display: grid; grid-template-columns: 1fr 1.8fr; gap: 16px; }

    /* SPARKLINE */
    .spark-section { background: var(--surface); border: 1px solid var(--border); padding: 24px; transition: background 0.2s, border-color 0.2s; }
    .spark-header { font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 500; letter-spacing: 0.12em; text-transform: uppercase; color: var(--text-muted); margin-bottom: 16px; display: flex; justify-content: space-between; }
    .spark-container { display: flex; align-items: flex-end; gap: 4px; height: 80px; }
    .spark-bar-wrap { flex: 1; height: 100%; display: flex; align-items: flex-end; position: relative; cursor: pointer; }
    .spark-bar { width: 100%; background: var(--spark-bar); transition: background 0.15s; border-radius: 1px 1px 0 0; }
    .spark-bar-wrap:hover .spark-bar { background: var(--spark-bar-hover); }
    .spark-empty { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--text-muted); display: flex; align-items: center; justify-content: center; width: 100%; height: 100%; }
    .spark-tooltip { display: none; position: absolute; bottom: 100%; left: 50%; transform: translateX(-50%); background: var(--text); color: var(--bg); font-family: 'IBM Plex Mono', monospace; font-size: 10px; padding: 4px 6px; white-space: nowrap; margin-bottom: 4px; z-index: 10; }
    .spark-bar-wrap:hover .spark-tooltip { display: block; }

    /* LOG */
    .log-section { background: var(--surface); border: 1px solid var(--border); padding: 24px; transition: background 0.2s, border-color 0.2s; }
    .log-header { font-family: 'IBM Plex Mono', monospace; font-size: 11px; font-weight: 500; letter-spacing: 0.12em; text-transform: uppercase; color: var(--text-muted); margin-bottom: 16px; display: flex; justify-content: space-between; align-items: center; }
    .log-table { width: 100%; border-collapse: collapse; font-family: 'IBM Plex Mono', monospace; font-size: 12px; }
    .log-table th { text-align: left; padding: 8px 12px; font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--text-muted); border-bottom: 1px solid var(--border); font-weight: 500; }
    .log-table td { padding: 10px 12px; border-bottom: 1px solid var(--border); color: var(--text-dim); vertical-align: middle; }
    .log-table tr:last-child td { border-bottom: none; }
    .log-table tr:hover td { background: rgba(128,128,128,0.04); }
    .status-pill { display: inline-flex; align-items: center; gap: 5px; padding: 3px 8px; font-size: 10px; font-weight: 500; letter-spacing: 0.08em; text-transform: uppercase; }
    .status-pill.ok { background: var(--pill-ok-bg); color: var(--green); }
    .status-pill.fail { background: var(--pill-fail-bg); color: var(--red); }
    .status-pill.running { background: var(--pill-run-bg); color: var(--amber); }

    /* SENSITIVE */
    .sensitive-cell { display: flex; align-items: center; gap: 8px; }
    .sensitive-value { color: var(--text); }
    .eye-btn { cursor: pointer; background: none; border: none; padding: 2px; color: var(--text-muted); display: flex; align-items: center; transition: color 0.15s; flex-shrink: 0; }
    .eye-btn:hover { color: var(--red); }
    .eye-btn svg { width: 14px; height: 14px; fill: none; stroke: currentColor; stroke-width: 1.5; stroke-linecap: round; stroke-linejoin: round; }
    .masked { letter-spacing: 0.15em; color: var(--text-muted); }

    footer { border-top: 1px solid var(--border); padding: 16px 40px; display: flex; justify-content: space-between; align-items: center; }
    .footer-text { font-family: 'IBM Plex Mono', monospace; font-size: 11px; color: var(--text-muted); }
    .footer-text span { color: var(--red); }

    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr 1fr; }
      .bottom-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 600px) {
      main { padding: 20px; } header { padding: 16px 20px; } .hero { padding: 24px; }
      .grid { grid-template-columns: 1fr 1fr; } .hero-number { font-size: 48px; }
      .refresh-info { display: none; }
    }
  </style>
</head>
<body>
<header>
  <div class="logo">
    <img src="/assets/nrs-logo.jpg" alt="NRS" onerror="this.style.display='none'" />
    <div class="logo-divider"></div>
    <div class="logo-text">
      <strong>E-Invoice Pipeline</strong>
      Data Intelligence Monitor
    </div>
  </div>
  <div class="header-right">
    <div class="live-badge"><div class="live-dot"></div>Live</div>
    <div class="refresh-info" id="refresh-info">Refreshing in 30s</div>
    <button class="theme-toggle" id="theme-toggle" title="Toggle light/dark mode">
      <svg id="theme-icon-moon" viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1111.21 3a7 7 0 109.79 9.79z"/></svg>
      <svg id="theme-icon-sun" viewBox="0 0 24 24" style="display:none"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3" stroke="currentColor" stroke-width="2"/><line x1="12" y1="21" x2="12" y2="23" stroke="currentColor" stroke-width="2"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64" stroke="currentColor" stroke-width="2"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78" stroke="currentColor" stroke-width="2"/><line x1="1" y1="12" x2="3" y2="12" stroke="currentColor" stroke-width="2"/><line x1="21" y1="12" x2="23" y2="12" stroke="currentColor" stroke-width="2"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36" stroke="currentColor" stroke-width="2"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22" stroke="currentColor" stroke-width="2"/></svg>
      <span id="theme-label">Light</span>
    </button>
  </div>
</header>

<main>
  <div class="hero">
    <div class="hero-label">Total Rows Inserted — NRS_EDW / dbo.factInvoice</div>
    <div class="hero-number" id="rows-inserted">—</div>
    <div class="hero-sub" id="rows-sub">Loading...</div>
    <div class="progress-section">
      <div class="progress-header">
        <div class="progress-label">Ingestion Progress</div>
        <div class="progress-pct" id="pct">—%</div>
      </div>
      <div class="progress-track"><div class="progress-fill" id="progress-fill" style="width:0%"></div></div>
      <div class="progress-ends">
        <span>0</span>
        <span id="total-source-label">—</span>
      </div>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="card-label">Batches Completed</div>
      <div class="card-value" id="batches">—</div>
      <div class="card-sub">250,000 rows per batch</div>
    </div>
    <div class="card">
      <div class="card-label">Failed Ranges</div>
      <div class="card-value" id="failed-count">—</div>
      <div class="card-sub">Retried automatically</div>
    </div>
    <div class="card">
      <div class="card-label">Current Speed</div>
      <div class="card-value red" id="speed">—</div>
      <div class="card-sub">rows / second (rolling avg)</div>
    </div>
    <div class="card">
      <div class="card-label">Est. Completion</div>
      <div class="card-value" id="eta" style="font-size:16px;line-height:1.4">—</div>
      <div class="card-sub" id="eta-sub">Based on current speed</div>
    </div>
  </div>

  <div class="bottom-grid">
    <div class="spark-section">
      <div class="spark-header">
        <span>Speed Trend</span>
        <span id="spark-label">Last 10 batches</span>
      </div>
      <div class="spark-container" id="sparkline">
        <div class="spark-empty">No batch data yet</div>
      </div>
    </div>

    <div class="log-section">
      <div class="log-header">
        <span>Pipeline Activity</span>
        <span id="last-updated">—</span>
      </div>
      <table class="log-table">
        <thead><tr><th>Metric</th><th>Value</th><th>Status</th></tr></thead>
        <tbody id="log-body"><tr><td colspan="3" style="color:var(--text-muted);text-align:center;padding:24px">Loading...</td></tr></tbody>
      </table>
    </div>
  </div>
</main>

<footer>
  <div class="footer-text">NRS Data Intelligence — <span>SMO</span></div>
  <div class="footer-text" id="started-at">—</div>
</footer>

<script>
  // ── THEME ──
  const html = document.documentElement;
  const savedTheme = localStorage.getItem('nrs-theme') || 'dark';
  html.setAttribute('data-theme', savedTheme);
  updateThemeUI(savedTheme);
  document.getElementById('theme-toggle').addEventListener('click', () => {
    const next = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
    localStorage.setItem('nrs-theme', next);
    updateThemeUI(next);
  });
  function updateThemeUI(theme) {
    document.getElementById('theme-icon-moon').style.display = theme === 'dark' ? 'block' : 'none';
    document.getElementById('theme-icon-sun').style.display = theme === 'light' ? 'block' : 'none';
    document.getElementById('theme-label').textContent = theme === 'dark' ? 'Light' : 'Dark';
  }

  // ── SENSITIVE ──
  const sensitive = { sourceDB: true, destination: true };
  function eyeIcon(id) {
    return '<button class="eye-btn" onclick="toggleSensitive(\\''+id+'\\')" title="Show/hide">' +
      '<svg viewBox="0 0 24 24"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg></button>';
  }
  function toggleSensitive(id) { sensitive[id] = !sensitive[id]; renderTable(window._lastData); }
  function sensitiveCell(id, val) {
    const display = sensitive[id] ? '<span class="masked">••••••••••••</span>' : val;
    return '<div class="sensitive-cell"><span class="sensitive-value">' + display + '</span>' + eyeIcon(id) + '</div>';
  }

  // ── UTILS ──
  const fmt = n => n == null ? '—' : Number(n).toLocaleString();
  const timeAgo = iso => { if (!iso) return '—'; const d = Math.floor((Date.now()-new Date(iso).getTime())/1000); return d<60?d+'s ago':d<3600?Math.floor(d/60)+'m ago':Math.floor(d/3600)+'h ago'; };
  const fmtDate = iso => iso ? new Date(iso).toLocaleString('en-GB',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';

  function fmtEta(ms) {
    if (!ms) return '—';
    const diff = ms - Date.now();
    if (diff <= 0) return 'Completing soon';
    const h = Math.floor(diff / 3600000);
    const m = Math.floor((diff % 3600000) / 60000);
    if (h > 24) {
      const days = Math.floor(h / 24);
      return days + 'd ' + (h % 24) + 'h remaining';
    }
    return h + 'h ' + m + 'm remaining';
  }

  function fmtEtaDate(ms) {
    if (!ms) return '';
    return 'Finishes ~' + new Date(ms).toLocaleString('en-GB',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'});
  }

  // ── SPARKLINE ──
  function renderSparkline(sparkData) {
    const el = document.getElementById('sparkline');
    if (!sparkData || sparkData.length === 0) {
      el.innerHTML = '<div class="spark-empty">No batch data yet</div>';
      return;
    }
    const max = Math.max(...sparkData.map(s => s.rps));
    el.innerHTML = sparkData.map(s => {
      const pct = max > 0 ? Math.max(4, Math.round((s.rps / max) * 100)) : 4;
      const time = fmtDate(s.completedAt);
      return '<div class="spark-bar-wrap">' +
        '<div class="spark-tooltip">' + s.rps.toLocaleString() + ' r/s<br>' + time + '</div>' +
        '<div class="spark-bar" style="height:' + pct + '%"></div>' +
        '</div>';
    }).join('');
    document.getElementById('spark-label').textContent = 'Last ' + sparkData.length + ' batch' + (sparkData.length > 1 ? 'es' : '');
  }

  // ── TABLE ──
  function renderTable(d) {
    if (!d) return;
    window._lastData = d;
    const remaining = (d.totalSourceRows && d.totalRowsInserted) ? d.totalSourceRows - d.totalRowsInserted : null;
    const rows = [
      ['Pipeline Status', 'Active', 'ok', false, null],
      ['Source DB', 'eivc @ 10.8.3.196', d.sourceUnreachable ? 'fail' : 'ok', true, 'sourceDB'],
      ['Destination', 'NRS_EDW / dbo.factInvoice', d.destUnreachable ? 'fail' : 'ok', true, 'destination'],
      ['Rows Inserted', fmt(d.totalRowsInserted), d.destUnreachable ? 'fail' : 'ok', false, null],
      ['Rows Remaining', fmt(remaining), remaining !== null && remaining > 0 ? 'running' : 'ok', false, null],
      ['Current Speed', d.avgRowsPerSecond ? d.avgRowsPerSecond.toLocaleString() + ' rows/s' : '—', d.avgRowsPerSecond ? 'ok' : 'running', false, null],
      ['ETA', fmtEtaDate(d.etaMs) || '—', d.etaMs ? 'ok' : 'running', false, null],
      ['Failed Ranges', fmt(d.failedRanges), d.failedRanges > 0 ? 'fail' : 'ok', false, null],
      ['Last Updated', timeAgo(d.lastUpdatedAt), 'ok', false, null],
    ];
    document.getElementById('log-body').innerHTML = rows.map(([l,v,s,isSensitive,sid]) => {
      const cell = isSensitive ? sensitiveCell(sid, v) : '<span style="color:var(--text)">' + v + '</span>';
      const pill = s==='ok'?'✓ OK':s==='fail'?'✗ FAIL':'⟳ ACTIVE';
      return '<tr><td>' + l + '</td><td>' + cell + '</td><td><span class="status-pill ' + s + '">' + pill + '</span></td></tr>';
    }).join('');
  }

  // ── FETCH ──
  let countdown = 30;
  async function fetchStatus() {
    try {
      const d = await fetch('/api/status').then(r => r.json());
      const pct = d.percentComplete ? parseFloat(d.percentComplete) : 0;
      const remaining = d.totalSourceRows && d.totalRowsInserted ? d.totalSourceRows - d.totalRowsInserted : null;

      document.getElementById('rows-inserted').textContent = d.destUnreachable ? 'Unreachable' : fmt(d.totalRowsInserted);
      document.getElementById('rows-sub').textContent = d.destUnreachable
        ? 'Could not connect to NRS_EDW'
        : fmt(remaining) + ' rows remaining of ' + fmt(d.totalSourceRows) + ' total';

      document.getElementById('total-source-label').textContent = d.sourceUnreachable
        ? 'Source unreachable'
        : fmt(d.totalSourceRows) + ' total source rows';

      document.getElementById('pct').textContent = d.percentComplete ? pct.toFixed(1) + '%' : '—%';
      document.getElementById('progress-fill').style.width = Math.min(pct, 100) + '%';
      document.getElementById('batches').textContent = fmt(d.totalBatchesCompleted);

      const fe = document.getElementById('failed-count');
      fe.textContent = fmt(d.failedRanges);
      fe.className = 'card-value ' + (d.failedRanges > 0 ? 'amber' : 'green');

      document.getElementById('speed').textContent = d.avgRowsPerSecond ? d.avgRowsPerSecond.toLocaleString() : '—';
      document.getElementById('eta').textContent = fmtEta(d.etaMs);
      document.getElementById('eta-sub').textContent = d.etaMs ? fmtEtaDate(d.etaMs) : 'Waiting for batch data';
      document.getElementById('last-updated').textContent = 'Last updated ' + timeAgo(d.lastUpdatedAt);
      document.getElementById('started-at').textContent = 'Started ' + fmtDate(d.startedAt);

      renderSparkline(d.sparkline);
      renderTable(d);
    } catch(e) {
      document.getElementById('rows-sub').textContent = 'Connection error — pipeline may be offline';
    }
  }

  function tick() {
    countdown--;
    document.getElementById('refresh-info').textContent = countdown <= 0 ? 'Refreshing...' : 'Refreshing in ' + countdown + 's';
    if (countdown <= 0) { countdown = 30; fetchStatus(); }
  }

  fetchStatus();
  setInterval(tick, 1000);
</script>
</body>
</html>`);
});

export function startApi(): void {
  app.listen(PORT, () => {
    log("Status dashboard running at http://localhost:" + PORT);
  });
}
