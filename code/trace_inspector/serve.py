"""
Trace Inspector — standalone tool, no Django required.

Reads directly from the project SQLite DB and serves a local web UI
for browsing trace verification results (annotated / invalid / unannotated).

Usage:
    cd paper_to_practice/trace_inspector
    python serve.py              # opens on http://localhost:8765
    python serve.py --port 9000  # custom port

Requirements: Python 3.11+ stdlib only (sqlite3, http.server, base64, pathlib).
"""

import argparse
import base64
import html
import json
import mimetypes
import re
import sqlite3
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# ── Paths (relative to this file, which lives inside the Django project) ───────
HERE       = Path(__file__).resolve().parent
PROJECT    = HERE.parent
DB_PATH    = PROJECT / "db.sqlite3"
MEDIA_ROOT = PROJECT / "media"

PER_PAGE = 40

# ── VIS_TYPES (mirrors core/taxonomy.py — kept in sync manually) ──────────────
VIS_TYPES = [
    "Bar","Grouped Bar","Stacked Bar","Histogram",
    "Line","Multi-Line","Stream Graph",
    "Scatter","Bubble","Dot Plot",
    "Area","Stacked Area",
    "Choropleth","Dot Map","Flow Map","Cartogram",
    "Node-Link","Adjacency Matrix","Arc Diagram","Chord Diagram",
    "Treemap","Sunburst","Dendrogram","Icicle",
    "Heatmap","Confusion Matrix",
    "Parallel Coordinates",
    "Glyph","Radar/Spider","Star Plot",
    "Sankey","Alluvial",
    "Small Multiples","Composite/Dashboard","Other",
]

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_traces(vis_type="", status="", page=1):
    conn = get_conn()
    params = []
    where  = []

    if vis_type:
        where.append("pf.vis_type = ?")
        params.append(vis_type)
    if status:
        where.append("t.annotation_status = ?")
        params.append(status)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    count_sql = f"""
        SELECT COUNT(*) as n
        FROM traces t
        JOIN paper_figures pf ON pf.id = t.figure_id
        JOIN papers p         ON p.id  = pf.paper_id
        JOIN repo_artifacts ra ON ra.id = t.artifact_id
        JOIN repo_sources rs   ON rs.id = ra.source_id
        {where_sql}
    """
    total = conn.execute(count_sql, params).fetchone()["n"]

    offset = (page - 1) * PER_PAGE
    data_sql = f"""
        SELECT
            t.id                   AS trace_id,
            t.annotation_status,
            t.invalid_reason,
            t.match_confidence,
            pf.id                  AS figure_id,
            pf.vis_type,
            pf.image_local_path,
            p.title                AS paper_title,
            p.year                 AS paper_year,
            p.doi                  AS paper_doi,
            p.track                AS paper_track,
            ra.id                  AS artifact_id,
            rs.title               AS nb_title,
            rs.url                 AS nb_url,
            rs.platform            AS nb_platform,
            rs.stars               AS nb_stars,
            da.encoding_drift,
            da.interaction_drift,
            da.task_drift,
            da.encoding_notes,
            da.interaction_notes,
            da.task_notes
        FROM traces t
        JOIN paper_figures pf  ON pf.id = t.figure_id
        JOIN papers p          ON p.id  = pf.paper_id
        JOIN repo_artifacts ra ON ra.id = t.artifact_id
        JOIN repo_sources rs   ON rs.id = ra.source_id
        LEFT JOIN drift_annotations da ON da.trace_id = t.id
        {where_sql}
        ORDER BY pf.vis_type, t.annotation_status, t.id DESC
        LIMIT {PER_PAGE} OFFSET {offset}
    """
    rows = conn.execute(data_sql, params).fetchall()
    conn.close()
    return total, [dict(r) for r in rows]


def fetch_summary():
    conn = get_conn()
    rows = conn.execute("""
        SELECT pf.vis_type, t.annotation_status, COUNT(*) as n
        FROM traces t
        JOIN paper_figures pf ON pf.id = t.figure_id
        GROUP BY pf.vis_type, t.annotation_status
        ORDER BY pf.vis_type, t.annotation_status
    """).fetchall()
    conn.close()

    summary = {}
    for r in rows:
        vt = r["vis_type"]
        if vt not in summary:
            summary[vt] = {"annotated": 0, "invalid": 0, "unannotated": 0}
        summary[vt][r["annotation_status"]] = r["n"]
    return summary


# ── Image helper ───────────────────────────────────────────────────────────────

def image_to_data_uri(image_local_path: str) -> str:
    if not image_local_path:
        return ""
    p = Path(image_local_path)
    if not p.is_absolute():
        p = MEDIA_ROOT / image_local_path
    if not p.exists():
        return ""
    try:
        mime = mimetypes.guess_type(str(p))[0] or "image/png"
        b64  = base64.b64encode(p.read_bytes()).decode()
        return f"data:{mime};base64,{b64}"
    except Exception:
        return ""


# ── HTML renderers ─────────────────────────────────────────────────────────────

def e(s):
    return html.escape(str(s or ""))


PLATFORM_STYLE = {
    "kaggle":       "background:#1a0d2d;color:#a056d3;border:1px solid #5b1e91",
    "github":       "background:#0d1117;color:#56b4d3;border:1px solid #1e6091",
    "observablehq": "background:#0a1a0f;color:#56d39b;border:1px solid #1e9157",
}
DRIFT_STYLE = {
    "none":  "background:#0d2d1a;color:#56d39b;border:1px solid #1e9157",
    "minor": "background:#2d1e00;color:#fbbf24;border:1px solid #854d0e",
    "major": "background:#2d0a0a;color:#f87171;border:1px solid #7f1d1d",
}
STATUS_STYLE = {
    "annotated":   "background:#0d2d1a;color:#56d39b;border:1px solid #1e9157",
    "invalid":     "background:#2d0a0a;color:#f87171;border:1px solid #7f1d1d",
    "unannotated": "background:#0d1117;color:#64748b;border:1px solid #1a2030",
}


def render_trace_card(t: dict) -> str:
    img_uri  = image_to_data_uri(t["image_local_path"])
    img_html = (
        f'<img class="trace-thumb" src="{img_uri}" style="width:100px;height:75px;object-fit:cover;'
        f'border-radius:3px;border:1px solid #1a2030;flex-shrink:0" alt="{e(t["vis_type"])}">'
        if img_uri else
        '<div style="width:100px;height:75px;border-radius:3px;border:1px solid #1a2030;'
        'background:#0d1117;flex-shrink:0;display:flex;align-items:center;justify-content:center;'
        'font-size:.6rem;color:#3d4f66">no img</div>'
    )

    doi_link = (
        f'<a href="https://doi.org/{e(t["paper_doi"])}" target="_blank" '
        f'style="color:#56b4d3;text-decoration:none">{e(t["paper_title"] or "")[:80]}</a>'
        if t["paper_doi"] else e((t["paper_title"] or "")[:80])
    )

    status_s = t["annotation_status"]
    sts = STATUS_STYLE.get(status_s, "")
    status_badge = (
        f'<span style="font-size:.6rem;letter-spacing:1.5px;text-transform:uppercase;'
        f'padding:1px 6px;border-radius:2px;{sts}">{e(status_s)}</span>'
    )

    vt_badge = (
        f'<span style="font-size:.62rem;letter-spacing:1px;padding:1px 5px;border-radius:2px;'
        f'background:#0d2d1a;color:#56d39b;border:1px solid #1e9157">{e(t["vis_type"])}</span>'
    )

    body = ""
    if status_s == "annotated":
        def dpill(dim, val):
            s = DRIFT_STYLE.get(val or "none", "")
            return (
                f'<span style="font-size:.6rem;letter-spacing:1px;text-transform:uppercase;'
                f'padding:1px 5px;border-radius:2px;{s}">{dim} {e(val or "none")}</span>'
            )
        body += (
            f'<div style="display:flex;gap:.35rem;flex-wrap:wrap;margin-bottom:.4rem">'
            f'{dpill("ENC", t["encoding_drift"])}'
            f'{dpill("INT", t["interaction_drift"])}'
            f'{dpill("TASK", t["task_drift"])}'
            f'</div>'
        )
        notes = []
        if t["encoding_notes"]:
            notes.append(f'<b style="color:#94a3b8">Enc:</b> {e(t["encoding_notes"])}')
        if t["interaction_notes"]:
            notes.append(f'<b style="color:#94a3b8">Int:</b> {e(t["interaction_notes"])}')
        if t["task_notes"]:
            notes.append(f'<b style="color:#94a3b8">Task:</b> {e(t["task_notes"])}')
        if notes:
            body += (
                f'<div style="font-size:.68rem;color:#64748b;line-height:1.55">'
                + '<br>'.join(notes) +
                '</div>'
            )
    elif status_s == "invalid":
        reason = t["invalid_reason"] or "Figure type unconfirmed by Gemini (reason not stored)"
        body += (
            f'<div style="font-size:.7rem;color:#f87171;font-style:italic;line-height:1.5">'
            f'{e(reason)}</div>'
        )
    else:
        body += '<div style="font-size:.68rem;color:#3d4f66;font-style:italic">Awaiting annotation</div>'

    platform = t["nb_platform"] or ""
    plat_s   = PLATFORM_STYLE.get(platform, "background:#0d1117;color:#64748b;border:1px solid #1a2030")
    plat_badge = (
        f'<span style="font-size:.6rem;letter-spacing:1px;text-transform:uppercase;'
        f'padding:1px 5px;border-radius:2px;flex-shrink:0;{plat_s}">{e(platform)}</span>'
    )
    stars = f'<span style="color:#3d4f66;font-size:.65rem;flex-shrink:0">★ {t["nb_stars"]}</span>' if t["nb_stars"] else ""
    nb_row = (
        f'<div style="display:flex;align-items:center;gap:.4rem;margin-top:.45rem;'
        f'padding-top:.45rem;border-top:1px solid #1a2030;font-size:.7rem;min-width:0">'
        f'{plat_badge}'
        f'<a href="{e(t["nb_url"])}" target="_blank" style="color:#56b4d3;text-decoration:none;'
        f'white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0">'
        f'{e((t["nb_title"] or "")[:55])}</a>'
        f'{stars}'
        f'</div>'
    )

    border_color = {
        "annotated":   "#1e9157",
        "invalid":     "#7f1d1d",
        "unannotated": "#1a2030",
    }.get(status_s, "#1a2030")

    # Serialize trace data for the modal — embedded as JSON in data attribute
    trace_data = json.dumps({
        "img_uri":           img_uri,
        "vis_type":          t["vis_type"],
        "status":            t["annotation_status"],
        "paper_title":       t["paper_title"] or "",
        "paper_year":        t["paper_year"] or "",
        "paper_track":       t["paper_track"] or "",
        "paper_doi":         t["paper_doi"] or "",
        "nb_title":          t["nb_title"] or "",
        "nb_url":            t["nb_url"] or "",
        "nb_platform":       t["nb_platform"] or "",
        "nb_stars":          t["nb_stars"] or "",
        "confidence":        t["match_confidence"],
        "invalid_reason":    t["invalid_reason"] or "",
        "encoding_drift":    t["encoding_drift"] or "",
        "interaction_drift": t["interaction_drift"] or "",
        "task_drift":        t["task_drift"] or "",
        "encoding_notes":    t["encoding_notes"] or "",
        "interaction_notes": t["interaction_notes"] or "",
        "task_notes":        t["task_notes"] or "",
    })

    return f"""
<div class="trace-card" style="border:1px solid #1a2030;border-left:3px solid {border_color};
            border-radius:6px;background:#0a0e14;overflow:hidden;cursor:pointer"
     data-trace="{e(trace_data)}"
     title="Double-click to expand">
  <div style="display:flex;gap:.65rem;padding:.65rem;border-bottom:1px solid #1a2030">
    {img_html}
    <div style="min-width:0;flex:1">
      <div style="font-size:.72rem;color:#56b4d3;line-height:1.4;
                  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:.2rem">
        {doi_link}
      </div>
      <div style="font-size:.65rem;color:#3d4f66">
        {e(t["paper_year"] or "—")}
        {("· " + e(t["paper_track"])) if t["paper_track"] and t["paper_track"] != "unknown" else ""}
      </div>
    </div>
  </div>
  <div style="padding:.6rem .65rem">
    <div style="display:flex;align-items:center;gap:.5rem;margin-bottom:.45rem;flex-wrap:wrap">
      {status_badge}
      {vt_badge}
    </div>
    {body}
    {nb_row}
  </div>
</div>"""


def render_summary_sidebar(summary: dict, vis_type: str, status: str) -> str:
    rows = ""
    totals = {"annotated": 0, "invalid": 0, "unannotated": 0}
    for vt, counts in sorted(summary.items()):
        for k in totals:
            totals[k] += counts.get(k, 0)
        ann  = counts.get("annotated", 0)
        inv  = counts.get("invalid", 0)
        unann= counts.get("unannotated", 0)
        rows += f"""
<div class="vt-item {'vt-active' if vt == vis_type else ''}"
     data-vt="{e(vt)}" data-st="">
  <div style="font-size:.68rem;color:#c9d1d9;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{e(vt)}</div>
  <div style="display:flex;gap:.3rem;margin-top:.15rem">
    <span style="font-size:.58rem;color:#56d39b">{ann}✓</span>
    <span style="font-size:.58rem;color:#f87171">{inv}✗</span>
    <span style="font-size:.58rem;color:#3d4f66">{unann}?</span>
  </div>
</div>"""

    total_row = (
        f'<div style="border-top:1px solid #1a2030;margin-top:.5rem;padding-top:.5rem;'
        f'font-size:.65rem;color:#3d4f66;display:flex;gap:.75rem">'
        f'<span style="color:#56d39b">{totals["annotated"]}✓</span>'
        f'<span style="color:#f87171">{totals["invalid"]}✗</span>'
        f'<span>{totals["unannotated"]}?</span>'
        f'</div>'
    )
    return f"""
<div style="width:200px;flex-shrink:0;background:#0a0e14;border:1px solid #1a2030;
            border-radius:6px;padding:.5rem;overflow-y:auto;max-height:calc(100vh - 160px)">
  <div style="font-size:.6rem;letter-spacing:2px;color:#3d4f66;
              text-transform:uppercase;margin-bottom:.5rem;padding:.2rem .5rem">Vis Type</div>
  <div class="vt-item {'vt-active' if not vis_type else ''}" data-vt="" data-st="">
    All types
  </div>
  {rows}
  {total_row}
</div>"""


def render_page(vis_type: str, status: str, page: int) -> str:
    total, rows = fetch_traces(vis_type, status, page)
    summary     = fetch_summary()
    total_pages = max((total + PER_PAGE - 1) // PER_PAGE, 1)

    cards = "\n".join(render_trace_card(t) for t in rows) if rows else (
        '<div style="color:#3d4f66;font-size:.8rem;padding:3rem;text-align:center">'
        'No traces match these filters.</div>'
    )

    status_buttons = ""
    for s, label in [("", "All"), ("annotated", "Annotated ✓"),
                     ("invalid", "Invalid ✗"), ("unannotated", "Pending ?")]:
        active = "background:#1e9157;color:#e6edf3;" if s == status else "background:#0d1117;color:#64748b;"
        status_buttons += (
            f'<button class="st-btn" data-st="{e(s)}" data-vt="{e(vis_type)}" '
            f'style="font-size:.7rem;padding:.3rem .75rem;border-radius:4px;border:1px solid #1a2030;'
            f'cursor:pointer;font-family:inherit;{active}">{label}</button>'
        )

    summary_sidebar = render_summary_sidebar(summary, vis_type, status)

    prev_btn = (
        f'<button onclick="goPage({page-1})" '
        f'style="font-size:.7rem;padding:.25rem .6rem;border-radius:4px;border:1px solid #1a2030;'
        f'background:#0d1117;color:#64748b;cursor:pointer;font-family:inherit">← prev</button>'
        if page > 1 else ""
    )
    next_btn = (
        f'<button onclick="goPage({page+1})" '
        f'style="font-size:.7rem;padding:.25rem .6rem;border-radius:4px;border:1px solid #1a2030;'
        f'background:#0d1117;color:#64748b;cursor:pointer;font-family:inherit">next →</button>'
        if page < total_pages else ""
    )
    pagination = (
        f'<div style="display:flex;gap:.5rem;align-items:center;margin-top:1.5rem;'
        f'justify-content:center;font-size:.72rem;color:#3d4f66">'
        f'{prev_btn}'
        f'<span>page {page} of {total_pages} &nbsp;·&nbsp; {total} traces</span>'
        f'{next_btn}'
        f'</div>'
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trace Inspector</title>
<style>
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:#080c10; color:#c9d1d9;
          font-family:'JetBrains Mono','Fira Code','Courier New',monospace;
          font-size:.875rem; }}
  a {{ color:#56b4d3; }}
  .vt-item {{
    padding:.3rem .5rem; border-radius:3px; cursor:pointer;
    font-size:.68rem; color:#64748b;
  }}
  .vt-item:hover {{ background:#0d2d1a; }}
  .vt-active {{ background:#0d2d1a; color:#c9d1d9; }}

  /* Modal */
  #img-modal {{
    display:none; position:fixed; inset:0; z-index:9999;
    background:rgba(0,0,0,.88);
    align-items:center; justify-content:center;
    cursor:zoom-out; padding:1rem;
  }}
  #img-modal.open {{ display:flex; }}
  .trace-card {{ cursor:pointer; }}
  .trace-card:hover {{ border-color:#2a3040 !important; }}
</style>
</head>
<body>
<div style="padding:1rem 1.5rem;border-bottom:1px solid #1a2030;
            display:flex;align-items:center;justify-content:space-between">
  <div>
    <span style="font-size:13px;font-weight:800;letter-spacing:2px;color:#e6edf3">
      TRACE INSPECTOR
    </span>
    <span style="font-size:.65rem;color:#3d4f66;letter-spacing:2px;margin-left:.75rem">
      GEMINI VERIFICATION RESULTS
    </span>
  </div>
  <div style="font-size:.65rem;color:#3d4f66">{DB_PATH}</div>
</div>

<div style="display:flex;gap:1rem;padding:1rem 1.5rem;align-items:flex-start">

  <!-- Sidebar -->
  {summary_sidebar}

  <!-- Main -->
  <div style="flex:1;min-width:0">

    <!-- Status filter -->
    <div style="display:flex;gap:.5rem;margin-bottom:1rem;flex-wrap:wrap">
      {status_buttons}
    </div>

    <!-- Grid -->
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:.75rem">
      {cards}
    </div>

    {pagination}
  </div>
</div>

<!-- Trace detail modal -->
<div id="img-modal">
  <div id="modal-inner" style="display:flex;gap:1.5rem;max-width:92vw;max-height:90vh;
       background:#0a0e14;border:1px solid #1a2030;border-radius:8px;
       overflow:hidden;box-shadow:0 0 60px rgba(0,0,0,.9)"
       onclick="event.stopPropagation()">

    <!-- Left: image -->
    <div id="modal-img-col" style="flex-shrink:0;display:flex;align-items:center;
         justify-content:center;background:#080c10;min-width:200px;max-width:55vw;padding:1rem">
      <img id="img-modal-img" src="" alt=""
           style="max-width:100%;max-height:calc(90vh - 2rem);border-radius:4px;
                  border:1px solid #1a2030;display:block">
    </div>

    <!-- Right: details -->
    <div id="modal-detail" style="flex:1;min-width:280px;max-width:420px;overflow-y:auto;
         padding:1.25rem 1.25rem 1.25rem 0;font-size:.78rem;line-height:1.6">
    </div>
  </div>
</div>

<script>
function applyFilter(vt, st) {{
  const p = new URLSearchParams();
  if (vt) p.set('vis_type', vt);
  if (st) p.set('status', st);
  p.set('page', '1');
  window.location.search = p.toString();
}}

function goPage(p) {{
  const params = new URLSearchParams(window.location.search);
  params.set('page', p);
  window.location.search = params.toString();
}}

// ── Modal ──────────────────────────────────────────────────────────────────────
const modal    = document.getElementById('img-modal');
const modalImg = document.getElementById('img-modal-img');
const modalDet = document.getElementById('modal-detail');
const modalImgCol = document.getElementById('modal-img-col');

const DRIFT_COLOR = {{ none:'#56d39b', minor:'#fbbf24', major:'#f87171' }};
const DRIFT_BG    = {{ none:'#0d2d1a', minor:'#2d1e00', major:'#2d0a0a' }};
const DRIFT_BORDER= {{ none:'#1e9157', minor:'#854d0e', major:'#7f1d1d' }};
const PLAT_COLOR  = {{ kaggle:'#a056d3', github:'#56b4d3', observablehq:'#56d39b' }};
const PLAT_BG     = {{ kaggle:'#1a0d2d', github:'#0d1117',  observablehq:'#0a1a0f' }};
const PLAT_BORDER = {{ kaggle:'#5b1e91', github:'#1e6091',  observablehq:'#1e9157' }};
const STATUS_COLOR= {{ annotated:'#56d39b', invalid:'#f87171', unannotated:'#64748b' }};
const STATUS_BG   = {{ annotated:'#0d2d1a', invalid:'#2d0a0a', unannotated:'#0d1117' }};
const STATUS_BORD = {{ annotated:'#1e9157', invalid:'#7f1d1d', unannotated:'#1a2030' }};

function pill(label, color, bg, border) {{
  return `<span style="font-size:.6rem;letter-spacing:1px;text-transform:uppercase;
    padding:2px 7px;border-radius:3px;
    color:${{color}};background:${{bg}};border:1px solid ${{border}}">${{label}}</span>`;
}}

function driftPill(dim, val) {{
  const v = val || 'none';
  return pill(`${{dim}} ${{v}}`, DRIFT_COLOR[v]||'#64748b', DRIFT_BG[v]||'#0d1117', DRIFT_BORDER[v]||'#1a2030');
}}

function row(label, value, color) {{
  if (!value) return '';
  return `<div style="margin-bottom:.5rem">
    <div style="font-size:.6rem;letter-spacing:1.5px;text-transform:uppercase;
                color:#3d4f66;margin-bottom:.15rem">${{label}}</div>
    <div style="color:${{color||'#c9d1d9'}}">${{value}}</div>
  </div>`;
}}

function divider() {{
  return '<div style="border-top:1px solid #1a2030;margin:.75rem 0"></div>';
}}

function buildDetail(t) {{
  const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  let html = '';

  // Status + vis_type
  const sc = STATUS_COLOR[t.status]||'#64748b', sb = STATUS_BG[t.status]||'#0d1117', sbo = STATUS_BORD[t.status]||'#1a2030';
  html += `<div style="display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:.85rem">
    ${{pill(t.status, sc, sb, sbo)}}
    ${{pill(t.vis_type, '#56d39b','#0d2d1a','#1e9157')}}
    <span style="font-size:.6rem;color:#3d4f66;align-self:center">confidence ${{(t.confidence||0).toFixed(2)}}</span>
  </div>`;

  // Paper
  const paperTitle = t.paper_doi
    ? `<a href="https://doi.org/${{esc(t.paper_doi)}}" target="_blank"
          style="color:#56b4d3;text-decoration:none">${{esc(t.paper_title)}}</a>`
    : esc(t.paper_title);
  html += row('Academic Paper', paperTitle);
  const meta = [t.paper_year, t.paper_track && t.paper_track !== 'unknown' ? t.paper_track : ''].filter(Boolean).join(' · ');
  if (meta) html += row('Year / Track', esc(meta), '#64748b');

  html += divider();

  // Drift / Invalid
  if (t.status === 'annotated') {{
    html += `<div style="margin-bottom:.5rem">
      <div style="font-size:.6rem;letter-spacing:1.5px;text-transform:uppercase;color:#3d4f66;margin-bottom:.4rem">Drift Assessment</div>
      <div style="display:flex;gap:.35rem;flex-wrap:wrap;margin-bottom:.6rem">
        ${{driftPill('ENC', t.encoding_drift)}}
        ${{driftPill('INT', t.interaction_drift)}}
        ${{driftPill('TASK', t.task_drift)}}
      </div>
    </div>`;
    if (t.encoding_notes)    html += row('Encoding', esc(t.encoding_notes), '#94a3b8');
    if (t.interaction_notes) html += row('Interaction', esc(t.interaction_notes), '#94a3b8');
    if (t.task_notes)        html += row('Task', esc(t.task_notes), '#94a3b8');
  }} else if (t.status === 'invalid') {{
    html += `<div style="margin-bottom:.5rem">
      <div style="font-size:.6rem;letter-spacing:1.5px;text-transform:uppercase;color:#3d4f66;margin-bottom:.3rem">Invalid Reason</div>
      <div style="color:#f87171;font-style:italic;line-height:1.55">
        ${{esc(t.invalid_reason || 'Figure type unconfirmed by Gemini (reason not stored)')}}
      </div>
    </div>`;
  }} else {{
    html += `<div style="color:#3d4f66;font-style:italic;font-size:.75rem">Awaiting Gemini annotation</div>`;
  }}

  html += divider();

  // Notebook
  const pc = PLAT_COLOR[t.nb_platform]||'#64748b';
  const pb = PLAT_BG[t.nb_platform]||'#0d1117';
  const pbo= PLAT_BORDER[t.nb_platform]||'#1a2030';
  html += `<div style="margin-bottom:.5rem">
    <div style="font-size:.6rem;letter-spacing:1.5px;text-transform:uppercase;color:#3d4f66;margin-bottom:.4rem">Repository Notebook</div>
    <div style="margin-bottom:.3rem">${{pill(t.nb_platform, pc, pb, pbo)}}</div>
    <div><a href="${{esc(t.nb_url)}}" target="_blank"
             style="color:#56b4d3;text-decoration:none;word-break:break-all">${{esc(t.nb_title)}}</a></div>
    ${{t.nb_stars ? `<div style="color:#3d4f66;font-size:.7rem;margin-top:.2rem">★ ${{t.nb_stars}}</div>` : ''}}
  </div>`;

  return html;
}}

function openModal(t) {{
  if (t.img_uri) {{
    modalImg.src = t.img_uri;
    modalImg.alt = t.vis_type;
    modalImgCol.style.display = 'flex';
  }} else {{
    modalImgCol.style.display = 'none';
  }}
  modalDet.innerHTML = buildDetail(t);
  modal.classList.add('open');
}}

function closeModal() {{
  modal.classList.remove('open');
  modalImg.src = '';
  modalDet.innerHTML = '';
}}

modal.addEventListener('click', function(e) {{
  if (e.target === modal) closeModal();
}});
document.addEventListener('keydown', function(e) {{
  if (e.key === 'Escape') closeModal();
}});

// Event delegation
document.addEventListener('click', function(e) {{
  const vtEl = e.target.closest('.vt-item');
  const stEl = e.target.closest('.st-btn');
  if (vtEl) applyFilter(vtEl.dataset.vt, {json.dumps(status)});
  else if (stEl) applyFilter(stEl.dataset.vt, stEl.dataset.st);
}});

// Auto-refresh — poll /count every 8s, reload page if trace count changed
(function() {{
  const INTERVAL = 8000;
  let lastCount = null;

  async function checkCount() {{
    try {{
      const r = await fetch('/count' + window.location.search);
      const d = await r.json();
      if (lastCount === null) {{ lastCount = d.total; return; }}
      if (d.total !== lastCount) window.location.reload();
    }} catch(_) {{}}
  }}

  setInterval(checkCount, INTERVAL);
  checkCount();
}})();
document.addEventListener('dblclick', function(e) {{
  const card = e.target.closest('.trace-card');
  if (!card || !card.dataset.trace) return;
  try {{
    openModal(JSON.parse(card.dataset.trace));
  }} catch(err) {{
    console.error('Modal parse error', err);
  }}
}});
</script>
</body>
</html>"""


# ── HTTP handler ───────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress default access log noise

    def handle_error(self, request, client_address):
        import sys
        if issubclass(sys.exc_info()[0], BrokenPipeError):
            return  # client disconnected mid-response — harmless
        super().handle_error(request, client_address)

    def do_GET(self):
        parsed = urlparse(self.path)
        qs     = parse_qs(parsed.query)

        vis_type = qs.get("vis_type", [""])[0]
        status   = qs.get("status",   [""])[0]
        try:
            page = max(int(qs.get("page", ["1"])[0]), 1)
        except ValueError:
            page = 1

        # Lightweight count endpoint for auto-refresh polling
        if parsed.path == "/count":
            try:
                total, _ = fetch_traces(vis_type, status, 1)
                body = json.dumps({"total": total}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                self.send_response(500)
                self.end_headers()
            return

        try:
            body = render_page(vis_type, status, page).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:
            err = f"<pre style='color:#f87171;padding:2rem'>{html.escape(str(exc))}</pre>".encode()
            self.send_response(500)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(err)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trace Inspector — standalone viewer")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}")
        raise SystemExit(1)

    url = f"http://localhost:{args.port}"
    server = HTTPServer(("localhost", args.port), Handler)

    print(f"Trace Inspector running at {url}")
    print(f"DB: {DB_PATH}")
    print("Press Ctrl+C to stop.\n")

    # Server is bound and ready — safe to open browser now
    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")