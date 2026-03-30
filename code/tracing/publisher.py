"""
Narrative publisher.

Renders a published Narrative to:
  - media/narratives/<slug>/index.html  (standalone, Dublin Core meta, base64 images)
  - media/narratives/<slug>/metadata.jsonld (schema.org/LearningResource + DC fields)

PDF generation is deferred (WeasyPrint not confirmed in environment).
The HTML includes a window.print()-friendly print stylesheet.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import date
from pathlib import Path

from django.conf import settings
from core.taxonomy import vis_type_to_slug

logger = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────────

def _slug(vis_type: str) -> str:
    """Convert vis_type to a filesystem-safe slug."""
    return vis_type_to_slug(vis_type)


def _image_to_data_uri(path: str) -> str:
    """Read an image file and return a base64 data URI. Returns '' on failure.

    Accepts both absolute paths and paths relative to MEDIA_ROOT (mirrors the
    JS getRelPath logic used in author.html: find 'media/' in the stored value
    and resolve from there).
    """
    try:
        p = Path(path)
        if not p.is_absolute():
            p = Path(settings.MEDIA_ROOT) / path
        data = p.read_bytes()
        ext = p.suffix.lower().lstrip(".")
        mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg",
                "png": "image/png", "webp": "image/webp"}.get(ext, "image/png")
        b64 = base64.b64encode(data).decode("utf-8")
        return f"data:{mime};base64,{b64}"
    except Exception as exc:
        logger.warning("publisher: could not inline image %s: %s", path, exc)
        return ""


def _dc_meta(name: str, content: str) -> str:
    return f'  <meta name="{name}" content="{_esc(content)}">\n'


def _esc(s: str) -> str:
    """Minimal HTML attribute escape."""
    return s.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")


def _html_esc(s: str) -> str:
    """Minimal HTML content escape."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Block renderers ────────────────────────────────────────────────────────────

def _render_text_block(block: dict) -> str:
    content = _html_esc(block.get("content", ""))
    # Preserve paragraph breaks
    paragraphs = content.split("\n\n")
    html_paras = "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paragraphs if p.strip())
    return (
        f'<section class="narrative-text section-card">\n'
        f'  <div class="section-kicker">Narrative</div>\n'
        f'  <div class="narrative-prose">{html_paras}</div>\n'
        f'</section>\n'
    )


def _render_query_prompt_block(block: dict) -> str:
    content = _html_esc(block.get("content", ""))
    return (
        f'<section class="query-prompt-block section-card">\n'
        f'  <p class="guidance-prompt-label">Author\'s focus</p>\n'
        f'  <blockquote class="guidance-prompt">{content}</blockquote>\n'
        f'</section>\n'
    )


def _render_chart_block(block: dict, idx: int) -> str:
    spec = block.get("plotly_spec", {})
    prompt = _html_esc(block.get("prompt", ""))
    chart_id = f"chart-{idx}"
    spec_json = json.dumps(spec)
    return (
        f'<section class="chart-block section-card">\n'
        f'  <div class="section-kicker">Evidence chart</div>\n'
        f'  <div id="{chart_id}" class="plotly-chart"></div>\n'
        f'  <p class="chart-prompt">{prompt}</p>\n'
        f'  <script type="application/json" data-chart="{chart_id}">{spec_json}</script>\n'
        f'</section>\n'
    )


def _render_figures_block(block: dict) -> str:
    """Render figures block with traced evidence tier and related figures tier."""
    traced_papers = block.get("traced_papers")
    related_papers = block.get("papers")

    # Fallback: legacy blocks have neither key — use flat metadata
    if not traced_papers and not related_papers:
        items = block.get("metadata", [])
        if not items:
            return ""
        cards = ""
        for fig in items:
            doi = fig.get("doi", "")
            doi_link = f'https://doi.org/{doi}' if doi else "#"
            title = _html_esc(fig.get("title", "Unknown"))
            year = fig.get("year", "")
            img_path = fig.get("image_local_path", "")
            data_uri = _image_to_data_uri(img_path) if img_path else ""
            img_tag = (
                f'<img src="{data_uri}" alt="{title}" class="fig-thumb">'
                if data_uri
                else '<div class="fig-thumb-placeholder"></div>'
            )
            cards += (
                f'<div class="fig-card">'
                f'  <a href="{doi_link}" target="_blank">{img_tag}</a>'
                f'  <div class="fig-meta"><a href="{doi_link}" target="_blank">{title}</a>'
                f'  <span class="fig-year">{year}</span></div>'
                f'</div>\n'
            )
        return (
            f'<section class="figures-block section-card">\n'
            f'  <div class="section-kicker">Academic evidence</div>\n'
            f'  <h3>Academic Sources</h3>\n'
            f'  <div class="fig-grid">{cards}</div>\n'
            f'</section>\n'
        )

    html = '<section class="figures-block section-card">\n'
    html += '  <div class="section-kicker">Academic evidence</div>\n'

    # Tier 1 — Traced figures (Gemini-verified annotated pairs)
    if traced_papers:
        html += (
            f'  <h3>Traced Academic Sources</h3>\n'
            f'  <p class="figures-tier-desc">Figures directly used in drift annotation — '
            f'verified by Gemini as genuine {block.get("vis_type", "this type")} visualizations.</p>\n'
        )
        html += f'  <div class="fig-paper-groups">\n'
        html += _render_paper_groups(traced_papers)
        html += f'  </div>\n'
    else:
        html += (
            f'  <h3>Traced Academic Sources</h3>\n'
            f'  <p class="figures-tier-desc figures-tier-empty">No verified traces yet for this type.</p>\n'
        )

    # Tier 2 — Related figures (broader evidence pool)
    if related_papers:
        html += (
            f'  <h3 style="margin-top:1.5rem">Other Related Academic Sources</h3>\n'
            f'  <p class="figures-tier-desc">Additional papers from the IEEE VIS corpus that contain this visualization type.</p>\n'
        )
        html += f'  <div class="fig-paper-groups">\n'
        html += _render_paper_groups(related_papers)
        html += f'  </div>\n'

    html += '</section>\n'
    return html


def _render_paper_groups(papers: list) -> str:
    """Render a list of paper group dicts as fig-paper-group HTML."""
    groups_html = ""
    for paper in papers:
        doi = paper.get("doi", "")
        doi_link = f'https://doi.org/{doi}' if doi else "#"
        title = _html_esc(paper.get("title", "Unknown"))
        year = paper.get("year", "") or ""
        figs = paper.get("figures", [])
        if not figs:
            continue
        thumbs = ""
        for fig in figs:
            img_path = fig.get("image_local_path", "")
            data_uri = _image_to_data_uri(img_path) if img_path else ""
            if data_uri:
                img_tag = f'<img src="{data_uri}" alt="{title}" class="fig-thumb">'
            else:
                img_tag = '<div class="fig-thumb-placeholder"></div>'
            thumbs += (
                f'<a href="{doi_link}" target="_blank" class="fig-thumb-link">'
                f'{img_tag}'
                f'</a>\n'
            )
        groups_html += (
            f'<div class="fig-paper-group">\n'
            f'  <div class="fig-paper-heading">'
            f'<a href="{doi_link}" target="_blank">{title}</a>'
            f'<span class="fig-year">&nbsp;{_html_esc(str(year))}</span>'
            f'</div>\n'
            f'  <div class="fig-thumb-row">{thumbs}</div>\n'
            f'</div>\n'
        )
    return groups_html




def _render_image_block(block: dict) -> str:
    prompt = _html_esc(block.get("prompt", ""))
    image_b64 = block.get("image_b64", "")
    error = block.get("_error", "")

    if error and not image_b64:
        return (
            f'<section class="chart-block section-card">\n'
            f'  <div class="section-kicker">Generated visual</div>\n'
            f'  <p style="color:#991b1b;font-size:.75rem">Chart error: {_html_esc(error)}</p>\n'
            f'  <p class="chart-prompt">{prompt}</p>\n'
            f'</section>\n'
        )

    return (
        f'<section class="chart-block section-card">\n'
        f'  <div class="section-kicker">Generated visual</div>\n'
        f'  <img src="{image_b64}" alt="{prompt}" style="max-width:100%;border-radius:4px;border:1px solid var(--border)">\n'
        f'  <p class="chart-prompt">{prompt}</p>\n'
        f'</section>\n'
    )


def _render_notebooks_block(block: dict) -> str:
    traced = block.get("traced_notebooks", [])
    related = block.get("metadata", [])
    if not traced and not related:
        return ""

    def _nb_rows(items: list) -> str:
        rows = ""
        for nb in items:
            platform = _html_esc(nb.get("platform", ""))
            title = _html_esc(nb.get("title", "Unknown"))
            url = nb.get("url", "#")
            stars = nb.get("stars", "")
            chart_types = ", ".join(nb.get("chart_types", []))
            rows += (
                f'<li class="notebook-item">'
                f'  <span class="platform-badge platform-{platform.lower()}">{platform}</span>'
                f'  <a href="{url}" target="_blank">{title}</a>'
                f'  {f"<span class=nb-stars>★ {stars}</span>" if stars else ""}'
                f'  {f"<span class=nb-types>{_html_esc(chart_types)}</span>" if chart_types else ""}'
                f'</li>\n'
            )
        return rows

    html = '<section class="notebooks-block section-card">\n'
    html += '  <div class="section-kicker">Repository evidence</div>\n'

    # Tier 1 — Traced notebooks
    html += '  <h3>Traced Repository Implementations</h3>\n'
    if traced:
        html += '  <p class="figures-tier-desc">Notebooks directly used in drift annotation — paired with verified academic figures.</p>\n'
        html += f'  <ul class="notebook-list">{_nb_rows(traced)}</ul>\n'
    else:
        html += '  <p class="figures-tier-desc figures-tier-empty">No verified traces yet for this type.</p>\n'

    # Tier 2 — Related notebooks
    if related:
        html += '  <h3 style="margin-top:1.5rem">Other Related Repository Implementations</h3>\n'
        html += '  <p class="figures-tier-desc">Other public notebooks that implement this visualization type.</p>\n'
        html += f'  <ul class="notebook-list">{_nb_rows(related)}</ul>\n'

    html += '</section>\n'
    return html


def _render_drift_evidence_block(block: dict) -> str:
    """
    Render an accordion with three panels (encoding / interaction / task).
    Each panel contains severity groups (major → minor → none), each with a
    scrollable list of annotation cards showing paper title, notebook link,
    and the LLM-generated justification notes.
    Uses <details>/<summary> — no JS dependency, print-friendly.
    """
    total = block.get("total", 0)
    totals = block.get("totals", {})
    dimensions = block.get("dimensions", {})

    _SEV_COLOR = {"major": "#991b1b", "minor": "#92400e", "none": "#0f766e"}
    _SEV_BG    = {"major": "#fef2f2", "minor": "#fffbeb", "none": "#f0fdfa"}
    _SEV_BORDER= {"major": "#fca5a5", "minor": "#f59e0b", "none": "#5eead4"}
    dim_labels = {
        "encoding": "Encoding design",
        "interaction": "Interaction model",
        "task": "Task framing",
    }
    sev_labels = {
        "major": "major shift",
        "minor": "minor shift",
        "none": "aligned",
    }

    html = f'<section class="drift-evidence section-card">\n'
    html += f'  <div class="section-kicker">Evidence review</div>\n'
    html += f'  <h3>Drift Evidence — {total} annotation{"s" if total != 1 else ""}</h3>\n'
    html += (
        '  <p class="drift-intro">'
        'Drift evidence compares traced repository examples against the research '
        'sources they inherit from. Each note records whether practice changed '
        'the original encoding, interaction pattern, or intended task.'
        '</p>\n'
    )
    html += (
        '  <div class="drift-legend">'
        '    <div class="drift-legend-item drift-legend-major"><strong>Major shift</strong> Substantial departure from the research intent.</div>'
        '    <div class="drift-legend-item drift-legend-minor"><strong>Minor shift</strong> Noticeable adaptation, but the original idea is still visible.</div>'
        '    <div class="drift-legend-item drift-legend-none"><strong>Aligned</strong> Practice stays close to the source framing.</div>'
        '  </div>\n'
    )

    for dim in ("encoding", "interaction", "task"):
        dim_totals = totals.get(dim, {})
        dim_data   = dimensions.get(dim, {})

        pills = []
        for sev in ("major", "minor", "none"):
            n = dim_totals.get(sev, 0)
            if n:
                pills.append(
                    f'<span class="sev-pill" style="'
                    f'background:{_SEV_BG[sev]};color:{_SEV_COLOR[sev]};'
                    f'border:1px solid {_SEV_BORDER[sev]};border-left:3px solid {_SEV_COLOR[sev]}'
                    f'">{n}&nbsp;{sev_labels[sev]}</span>'
                )
        summary_str = "".join(pills) if pills else '<span style="color:var(--muted);font-size:.75rem">no data</span>'

        html += f'  <details class="drift-accordion">\n'
        html += (
            f'    <summary class="drift-dim-summary">'
            f'<span class="drift-dim-name">{dim_labels[dim]}</span>'
            f'<span class="drift-dim-counts">{summary_str}</span>'
            f'</summary>\n'
        )
        html += f'    <div class="drift-dim-body">\n'

        for sev in ("major", "minor", "none"):
            entries = dim_data.get(sev, [])
            if not entries:
                continue
            html += f'      <div class="drift-sev-group">\n'
            html += (
                f'        <div class="drift-sev-label sev-{sev}">'
                f'{sev.upper()} ({len(entries)})</div>\n'
            )
            html += f'        <div class="drift-scroll">\n'
            for e in entries:
                paper    = _html_esc(e.get("paper_title", ""))
                nb_title = _html_esc(e.get("notebook_title", ""))
                nb_url   = e.get("notebook_url", "#")
                platform = e.get("platform", "")
                notes    = _html_esc(e.get("notes", "") or "—")
                html += (
                    f'          <div class="drift-card">\n'
                    f'            <div class="drift-card-paper">{paper}</div>\n'
                    f'            <div class="drift-card-notebook">'
                    f'<a href="{nb_url}" target="_blank">{nb_title}</a>'
                    f'{"&nbsp;" + f"<span class=platform-badge platform-{platform}>{platform}</span>" if platform else ""}'
                    f'</div>\n'
                    f'            <div class="drift-card-notes">{notes}</div>\n'
                    f'          </div>\n'
                )
            html += f'        </div>\n'  # drift-scroll
            html += f'      </div>\n'    # drift-sev-group

        html += f'    </div>\n'  # drift-dim-body
        html += f'  </details>\n'

    html += f'</section>\n'
    return html


# ── CSS ────────────────────────────────────────────────────────────────────────

STANDALONE_CSS = """
:root {
  --academic: #0369a1; --repo: #7c3aed; --bridge: #0f766e;
  --academic-bg: #e0f2fe; --repo-bg: #ede9fe; --bridge-bg: #f0fdfa;
  --bg: #f0f4f8; --surface: #ffffff; --surface2: #f8fafc; --border: #d1d9e6;
  --text: #1e293b; --text-dim: #334155; --heading: #0f172a; --muted: #475569;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
  font-size: 1rem; line-height: 1.7;
  margin: 0; padding: 2rem 1.25rem 4rem;
}
.page-shell {
  max-width: 980px;
  margin: 0 auto;
}
.back-link {
  display: inline-block; margin-bottom: 2rem;
  font-size: .75rem; letter-spacing: 2px; color: var(--muted);
  text-decoration: none; text-transform: uppercase;
}
.back-link:hover { color: var(--bridge); }
header {
  margin-bottom: 1.25rem;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 1.25rem 1.3rem;
  background: var(--surface);
  box-shadow: 0 1px 4px rgba(0,0,0,.06), 0 0 0 1px rgba(0,0,0,.03);
}
.header-topline {
  font-size: .72rem; color: var(--muted); letter-spacing: 2px;
  text-transform: uppercase; margin-bottom: .8rem;
}
header .vis-type {
  font-family: 'Syne', 'Arial Black', sans-serif;
  font-size: clamp(1.9rem, 3.6vw, 2.7rem); font-weight: 800; letter-spacing: 2px;
  color: var(--heading); text-transform: uppercase; margin-bottom: .5rem;
}
.header-meta {
  display: flex; flex-wrap: wrap; gap: .45rem;
  margin-bottom: .9rem;
}
.meta-chip {
  display: inline-flex; align-items: center;
  padding: .22rem .5rem;
  border-radius: 3px;
  border: 1px solid var(--border);
  background: var(--surface2);
  color: var(--muted);
  font-size: .7rem; letter-spacing: 1px; text-transform: uppercase;
}
.meta-chip.accent { border-color: #0f766e; color: var(--bridge); background: var(--bridge-bg); }
.header-deck {
  max-width: 70ch;
  color: var(--text-dim);
  font-size: .86rem;
  line-height: 1.75;
}
main { display: flex; flex-direction: column; gap: 1rem; }
section { margin-bottom: 0; }
.section-card {
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface);
  padding: 1rem 1.05rem;
  box-shadow: 0 1px 4px rgba(0,0,0,.04);
}
.section-kicker {
  font-size: .68rem; letter-spacing: 2px; color: var(--muted);
  text-transform: uppercase; margin-bottom: .6rem; font-weight: 700;
}
.narrative-prose p { margin-bottom: 1rem; color: var(--text); text-align: justify; hyphens: auto; }
.narrative-prose p:last-child { margin-bottom: 0; }
.narrative-prose p:first-child {
  font-size: 1.02rem;
  line-height: 1.85;
}
section h3 {
  font-size: .9rem; letter-spacing: 2.5px; color: var(--text-dim);
  text-transform: uppercase; margin-bottom: 1rem; font-weight: 800;
  border-bottom: 1px solid var(--border); padding-bottom: .55rem;
}
.plotly-chart { min-height: 300px; background: var(--surface);
  border: 1px solid var(--border); border-radius: 4px; }
.chart-prompt {
  font-size: .78rem; color: var(--muted); margin-top: .7rem;
  font-style: italic; padding: .55rem .65rem;
  border: 1px solid var(--border); border-radius: 4px; background: var(--surface2);
}
.fig-grid { display: flex; flex-wrap: wrap; gap: .75rem; }
.fig-card { width: 140px; }
.fig-thumb { width: 140px; height: 105px; object-fit: cover;
  border: 1px solid var(--border); border-radius: 3px; display: block; }
.fig-thumb-placeholder { width: 140px; height: 105px;
  background: var(--surface); border: 1px solid var(--border); border-radius: 3px; }
.fig-meta { font-size: .82rem; margin-top: .35rem; color: var(--muted); }
.fig-meta a { color: var(--academic); text-decoration: none; }
.fig-meta a:hover { text-decoration: underline; }
.fig-year { color: var(--muted); font-size: .82rem; flex-shrink: 0; }
/* Grouped figures layout */
.fig-paper-groups { display: flex; flex-direction: column; gap: 1.1rem; }
.fig-paper-group {}
.fig-paper-heading {
  font-size: .88rem; margin-bottom: .4rem; line-height: 1.4;
  display: flex; align-items: baseline; gap: .3rem;
}
.fig-paper-heading a {
  color: var(--academic); text-decoration: none; font-weight: 600;
  min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.fig-paper-heading a:hover { text-decoration: underline; }
.fig-thumb-row { display: flex; flex-wrap: wrap; gap: .4rem; }
.fig-thumb-link { display: block; flex-shrink: 0; }
.fig-thumb-link .fig-thumb { width: 110px; height: 82px; }
.notebook-list { list-style: none; }
.notebook-item { margin-bottom: .75rem; font-size: .88rem; }
.platform-badge {
  display: inline-block; font-size: .75rem; letter-spacing: 1px;
  padding: 2px 7px; border-radius: 3px; margin-right: .4rem;
  text-transform: uppercase; vertical-align: middle; font-weight: 700;
}
.platform-kaggle       { background: #ede9fe; color: #7c3aed; border: 1px solid #c4b5fd; border-left: 3px solid #7c3aed; }
.platform-github       { background: #e0f2fe; color: #0369a1; border: 1px solid #7dd3f0; border-left: 3px solid #0369a1; }
.platform-observablehq { background: #f0fdfa; color: #0f766e; border: 1px solid #5eead4; border-left: 3px solid #0f766e; }
.notebook-item a  { color: var(--academic); font-weight: 600; }
.nb-stars  { color: var(--muted); font-size: .82rem; margin-left: .4rem; }
.nb-types  { color: var(--muted); font-size: .75rem; margin-left: .4rem; }
.drift-summary table { border-collapse: collapse; width: 100%; font-size: .88rem; }
.drift-summary th, .drift-summary td {
  text-align: left; padding: .4rem .75rem;
  border-bottom: 1px solid var(--border);
}
.drift-summary th { color: var(--muted); font-size: .82rem; letter-spacing: 1.5px; text-transform: uppercase; font-weight: 700; }
.badge-none  { color: #0f766e; font-weight: 700; }
.badge-minor { color: #92400e; font-weight: 700; }
.badge-major { color: #991b1b; font-weight: 700; }
.figures-tier-desc {
  font-size: .82rem; color: var(--muted); margin-bottom: .75rem;
  font-style: italic; line-height: 1.5;
}
.figures-tier-empty { color: var(--muted); }
footer { margin-top: 3rem; padding-top: 1rem; border-top: 1px solid var(--border);
  font-size: .82rem; color: var(--muted); letter-spacing: 1px; }

/* ── Guidance prompt ── */
.guidance-prompt-label {
  font-size: .72rem; letter-spacing: 2px; text-transform: uppercase;
  color: var(--muted); margin-bottom: .45rem; font-weight: 700;
}
.guidance-prompt {
  border-left: 3px solid var(--bridge);
  margin: 0; padding: .75rem 1rem;
  font-style: italic; color: var(--muted);
  font-size: .88rem; line-height: 1.7;
  background: var(--surface2);
  border-radius: 0 4px 4px 0;
}

.drift-intro {
  margin: -.2rem 0 .85rem;
  color: var(--text-dim);
  font-size: .8rem;
  line-height: 1.65;
}
.drift-legend {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: .6rem;
  margin-bottom: 1rem;
}
.drift-legend-item {
  border: 1px solid var(--border);
  border-radius: 4px;
  background: var(--surface2);
  padding: .6rem .7rem;
  font-size: .74rem;
  line-height: 1.55;
  color: var(--muted);
}
.drift-legend-item strong {
  display: block;
  margin-bottom: .18rem;
  color: var(--heading);
  font-size: .72rem;
  letter-spacing: 1px;
  text-transform: uppercase;
}
.drift-legend-major { border-left: 3px solid #dc2626; }
.drift-legend-minor { border-left: 3px solid #d97706; }
.drift-legend-none  { border-left: 3px solid #0f766e; }

@media print {
  body { background: white; color: #111; max-width: 100%; margin: 0; }
  :root {
    --bg: white; --surface: #f8f8f8; --surface2: #f0f0f0; --border: #ccc;
    --text: #111; --muted: #555;
    --academic: #0369a1; --repo: #7c3aed; --bridge: #0f766e;
  }
  .page-shell { max-width: 100%; }
  .back-link { display: none; }
  .plotly-chart { page-break-inside: avoid; }
  /* Uncap every container that clips drift content */
  .drift-accordion { overflow: visible; }
  .drift-scroll { max-height: none; overflow: visible; }
  /* Let paper titles print in full */
  .drift-card-paper { white-space: normal; overflow: visible; text-overflow: unset; }
  /* Chips and pills print with their tinted backgrounds */
  .drift-sev-label, .sev-pill { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
  /* Hide the chevron */
  .drift-dim-summary::before { display: none; }
  /* Prevent blank pages from section bottom-margins stacking */
  section { margin-bottom: .75rem; }
}

/* ── Drift evidence accordion ── */
.drift-evidence { margin-bottom: 2.5rem; }
.drift-accordion {
  border: 1px solid var(--border); border-radius: 4px;
  margin-bottom: .5rem; overflow: hidden;
}
.drift-dim-summary {
  display: flex; align-items: center; gap: .75rem;
  padding: .65rem .85rem; cursor: pointer; list-style: none;
  background: #f1f5f9; border-bottom: 1px solid var(--border);
}
.drift-dim-summary::-webkit-details-marker { display: none; }
.drift-dim-summary::before {
  content: '▶'; font-size: .75rem; color: var(--muted);
  transition: transform .15s; display: inline-block; flex-shrink: 0;
}
details[open] > .drift-dim-summary::before { transform: rotate(90deg); }
.drift-dim-name {
  font-size: .9rem; letter-spacing: .3px; color: var(--heading);
  flex-shrink: 0; font-weight: 800;
}
.drift-dim-counts { display: flex; gap: .4rem; flex-wrap: wrap; align-items: center; }
.drift-dim-counts .sev-pill {
  font-size: .72rem; font-weight: 700; letter-spacing: .4px;
  padding: 1px 7px; border-radius: 3px;
}
.drift-dim-body { padding: .85rem .85rem; }
.drift-sev-group { margin-bottom: 1rem; }
.drift-sev-label {
  font-size: .82rem; letter-spacing: 2px; text-transform: uppercase;
  margin-bottom: .5rem; font-weight: 800;
  padding: 3px 10px; border-radius: 3px; display: inline-block;
}
.drift-sev-label.sev-major { background: #fef2f2; color: #991b1b; border: 1px solid #fca5a5; border-left: 4px solid #dc2626; }
.drift-sev-label.sev-minor { background: #fffbeb; color: #92400e; border: 1px solid #f59e0b; border-left: 4px solid #d97706; }
.drift-sev-label.sev-none  { background: #f0fdfa; color: #0f766e; border: 1px solid #5eead4; border-left: 4px solid #0f766e; }
.drift-scroll {
  max-height: 380px; overflow-y: auto;
  padding-right: 4px;
  scrollbar-width: thin; scrollbar-color: var(--border) transparent;
}
.drift-card {
  border: 1px solid var(--border); border-radius: 3px;
  padding: .5rem .65rem; margin-bottom: .4rem;
  background: var(--surface);
}
.drift-card-paper {
  font-size: .78rem; color: var(--academic); font-weight: 600;
  margin-bottom: .2rem;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.drift-card-notebook {
  font-size: .75rem; color: var(--muted); margin-bottom: .35rem;
}
.drift-card-notebook a { color: var(--repo); text-decoration: none; }
.drift-card-notebook a:hover { text-decoration: underline; }
.drift-card-notes { font-size: .82rem; color: var(--text); line-height: 1.55; }

@media (max-width: 720px) {
  body { padding: 1rem .8rem 3rem; }
  header { padding: 1rem; }
  .section-card { padding: .9rem; }
  .header-meta { gap: .35rem; }
  .meta-chip { font-size: .66rem; }
  .narrative-prose p:first-child { font-size: .96rem; }
  .drift-legend { grid-template-columns: 1fr; }
}
"""


# ── Main render ────────────────────────────────────────────────────────────────

def render_html(narrative) -> str:
    """Render a complete standalone HTML document from a Narrative instance."""
    from tracing.models import DriftAnnotation, Trace

    vis_type = narrative.vis_type
    blocks = narrative.get_blocks()
    today = date.today().isoformat()
    excerpt = narrative.get_text_excerpt()

    # Drift summary for the drift evidence section
    drift_qs = DriftAnnotation.objects.filter(
        trace__figure__vis_type=vis_type,
        trace__verified=True,
    )
    enc = {"none": 0, "minor": 0, "major": 0}
    inter = {"none": 0, "minor": 0, "major": 0}
    task = {"none": 0, "minor": 0, "major": 0}
    for da in drift_qs:
        enc[da.encoding_drift] = enc.get(da.encoding_drift, 0) + 1
        inter[da.interaction_drift] = inter.get(da.interaction_drift, 0) + 1
        task[da.task_drift] = task.get(da.task_drift, 0) + 1
    total_annotations = drift_qs.count()

    # ── Dublin Core meta tags ──────────────────────────────────────────────────
    canonical_url = f"/narratives/{_slug(vis_type)}/{narrative.pk}/"
    dc_block = (
        _dc_meta("DC.title", f"{vis_type} — Design Drift Narrative")
        + _dc_meta("DC.creator", "Anonymous")
        + _dc_meta("DC.subject", f"data visualization, {vis_type}, design drift, visualization literacy")
        + _dc_meta("DC.description", narrative.get_text_excerpt())
        + _dc_meta("DC.date", today)
        + _dc_meta("DC.rights", "CC BY 4.0 — https://creativecommons.org/licenses/by/4.0/")
        + _dc_meta("DC.identifier", canonical_url)
        + _dc_meta("DC.type", "LearningResource")
        + _dc_meta("DC.format", "text/html")
        + _dc_meta("DC.language", "en")
    )

    # ── Body blocks ────────────────────────────────────────────────────────────
    body_html = ""
    chart_idx = 0
    has_drift_evidence_block = any(b.get("type") == "drift_evidence" for b in blocks)
    for block in blocks:
        btype = block.get("type")
        if btype == "text":
            body_html += _render_text_block(block)
        elif btype == "query_prompt":
            body_html += _render_query_prompt_block(block)
        elif btype == "chart":
            body_html += _render_chart_block(block, chart_idx)
            chart_idx += 1
        elif btype == "image":
            body_html += _render_image_block(block)
        elif btype == "figures":
            body_html += _render_figures_block(block)
        elif btype == "notebooks":
            body_html += _render_notebooks_block(block)
        elif btype == "drift_evidence":
            body_html += _render_drift_evidence_block(block)

    # ── Drift evidence section (legacy fallback for pre-block narratives) ──────
    # Skipped when the narrative already contains a drift_evidence block.
    drift_section = ""
    if not has_drift_evidence_block:
        def _sev_cell(counts: dict, level: str) -> str:
            n = counts.get(level, 0)
            return f'<td class="badge-{level}">{n}</td>'

        drift_section = f"""
<section class="drift-summary section-card">
  <div class="section-kicker">Evidence review</div>
  <h3>Drift Evidence ({total_annotations} annotations)</h3>
  <p class="drift-intro">
    Drift evidence compares traced repository examples against the research
    sources they inherit from. Major indicates a substantial departure, minor
    a noticeable adaptation, and aligned means practice stays close to the
    source framing.
  </p>
  <table>
    <thead><tr><th>Dimension</th><th>None</th><th>Minor</th><th>Major</th></tr></thead>
    <tbody>
      <tr><td>Encoding</td>    {_sev_cell(enc,'none')}   {_sev_cell(enc,'minor')}   {_sev_cell(enc,'major')}</tr>
      <tr><td>Interaction</td> {_sev_cell(inter,'none')} {_sev_cell(inter,'minor')} {_sev_cell(inter,'major')}</tr>
      <tr><td>Task</td>        {_sev_cell(task,'none')}  {_sev_cell(task,'minor')}  {_sev_cell(task,'major')}</tr>
    </tbody>
  </table>
</section>
"""

    # ── Chart boot script ──────────────────────────────────────────────────────
    chart_boot = """
<script>
document.querySelectorAll('script[data-chart]').forEach(function(el) {
  var id = el.getAttribute('data-chart');
  var container = document.getElementById(id);
  if (!container) return;
  try {
    var spec = JSON.parse(el.textContent);
    var layout = Object.assign({
      paper_bgcolor: 'rgba(248,250,252,0)', plot_bgcolor: 'rgba(248,250,252,0)',
      font: { color: '#475569', family: "'JetBrains Mono',monospace", size: 12 },
      margin: { l: 50, r: 20, t: 30, b: 50 }
    }, spec.layout || {});
    Plotly.newPlot(id, spec.data || [], layout, { responsive: true });
  } catch(e) { container.innerHTML = '<p style="color:#991b1b;padding:1rem">Chart render error</p>'; }
});

// ── Print: expand all drift accordions, restore after ─────────────────────
(function() {
  var _wasOpen = [];
  var _scrollStyles = [];
  window.addEventListener('beforeprint', function() {
    // 1. Open all <details> elements
    _wasOpen = [];
    document.querySelectorAll('details').forEach(function(el) {
      _wasOpen.push(el.hasAttribute('open'));
      el.setAttribute('open', '');
    });
    // 2. Remove max-height/overflow caps on scroll containers inline —
    //    CSS @media print overrides don't reliably win against stylesheet
    //    rules at the same specificity in all browsers.
    _scrollStyles = [];
    document.querySelectorAll('.drift-scroll, .drift-accordion').forEach(function(el) {
      _scrollStyles.push({
        el: el,
        maxHeight: el.style.maxHeight,
        overflow: el.style.overflow,
        overflowY: el.style.overflowY,
      });
      el.style.maxHeight = 'none';
      el.style.overflow  = 'visible';
      el.style.overflowY = 'visible';
    });
    // 3. Remove body bottom padding that causes blank trailing pages
    document.body.style.paddingBottom = '0';
  });
  window.addEventListener('afterprint', function() {
    document.querySelectorAll('details').forEach(function(el, i) {
      if (!_wasOpen[i]) el.removeAttribute('open');
    });
    _wasOpen = [];
    _scrollStyles.forEach(function(s) {
      s.el.style.maxHeight = s.maxHeight;
      s.el.style.overflow  = s.overflow;
      s.el.style.overflowY = s.overflowY;
    });
    _scrollStyles = [];
    document.body.style.paddingBottom = '';
  });
})();
</script>
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_esc(vis_type)} — Design Drift Narrative</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;600&family=Syne:wght@700;800&display=swap" rel="stylesheet">
  <script src="https://cdn.plot.ly/plotly-2.32.0.min.js" charset="utf-8"></script>
{dc_block}  <style>{STANDALONE_CSS}</style>
</head>
<body>
  <div class="page-shell">
  <a class="back-link" href="/narratives/">← Back to narratives</a>
  <header>
    <div class="header-topline">From Paper to Practice · Published narrative</div>
    <div class="vis-type">{_esc(vis_type)}</div>
    <div class="header-meta">
      <span class="meta-chip accent">IEEE VIS 2026</span>
      <span class="meta-chip">Generated {today}</span>
      <span class="meta-chip">{len(blocks)} block{"s" if len(blocks) != 1 else ""}</span>
      <a class="meta-chip" href="/narratives/jsonld/{narrative.pk}/" style="color:var(--muted)">JSON-LD ↗</a>
    </div>
    <div class="header-deck">{_html_esc(excerpt)}</div>
  </header>

  <main>
    {body_html}
    {drift_section}
  </main>

  <footer>
    Generated by From Paper to Practice
    · CC BY 4.0 · <a href="/narratives/" style="color:var(--muted)">narratives gallery</a>
  </footer>
  {chart_boot}
</div>
</body>
</html>"""


def render_jsonld(narrative) -> str:
    """Render a JSON-LD document (schema.org LearningResource + Dublin Core)."""
    vis_type = narrative.vis_type
    today = date.today().isoformat()
    slug = _slug(vis_type)
    excerpt = narrative.get_text_excerpt()
    n_figures = len(narrative.get_source_figures())
    n_artifacts = len(narrative.get_source_artifacts())

    doc = {
        "@context": {
            "@vocab": "https://schema.org/",
            "dc": "http://purl.org/dc/elements/1.1/",
        },
        "@type": "LearningResource",
        "name": f"{vis_type} — Design Drift Narrative",
        "description": excerpt,
        "keywords": ["data visualization", vis_type, "design drift",
                     "visualization literacy", "IEEE VIS", "OER"],
        "author": {
            "@type": "Organization",
            "name": "Anonymous",
        },
        "dateCreated": today,
        "license": "https://creativecommons.org/licenses/by/4.0/",
        "url": f"/narratives/{slug}/{narrative.pk}/",
        "educationalLevel": "Advanced",
        "learningResourceType": "narrative",
        "dc:title": f"{vis_type} — Design Drift Narrative",
        "dc:creator": "Anonymous",
        "dc:subject": f"data visualization, {vis_type}, design drift",
        "dc:description": excerpt,
        "dc:date": today,
        "dc:rights": "CC BY 4.0 — https://creativecommons.org/licenses/by/4.0/",
        "dc:identifier": f"/narratives/{slug}/{narrative.pk}/",
        "dc:format": "text/html",
        "dc:language": "en",
        "dc:type": "LearningResource",
        "additionalProperty": [
            {"@type": "PropertyValue", "name": "sourceAcademicFigures", "value": n_figures},
            {"@type": "PropertyValue", "name": "sourceRepositoryNotebooks", "value": n_artifacts},
            {"@type": "PropertyValue", "name": "modelUsed", "value": narrative.model_used},
        ],
    }
    return json.dumps(doc, indent=2, ensure_ascii=False)


def publish(narrative) -> dict[str, str]:
    """
    Render and write HTML + JSON-LD to media/narratives/<slug>/<id>/.
    Including the narrative primary key in the path means multiple narratives
    for the same vis_type each get their own directory with no collision.
    Returns dict with keys html_path, json_ld_path.
    Does NOT set narrative.status — caller is responsible.
    """
    slug = _slug(narrative.vis_type)
    out_dir = Path(settings.MEDIA_ROOT) / "narratives" / slug / str(narrative.pk)
    out_dir.mkdir(parents=True, exist_ok=True)

    html_content = render_html(narrative)
    html_file = out_dir / "index.html"
    html_file.write_text(html_content, encoding="utf-8")

    jsonld_content = render_jsonld(narrative)
    jsonld_file = out_dir / "metadata.jsonld"
    jsonld_file.write_text(jsonld_content, encoding="utf-8")

    # Paths relative to MEDIA_ROOT for storage in the model
    rel_html  = str(Path("narratives") / slug / str(narrative.pk) / "index.html")
    rel_jsonld = str(Path("narratives") / slug / str(narrative.pk) / "metadata.jsonld")

    logger.info("publisher: wrote %s and %s", html_file, jsonld_file)
    return {"html_path": rel_html, "json_ld_path": rel_jsonld}
