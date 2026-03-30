"""
Narrative authoring views.

Page views
----------
GET  /narratives/                              → gallery
GET  /narratives/<vis_type>/                   → detail (published doc or redirect)
GET  /narratives/<vis_type>/author/            → authoring page
GET  /narratives/<vis_type>/<id>/             → specific narrative by id
GET  /narratives/jsonld/<id>/                  → serve JSON-LD sidecar by id

API endpoints (all POST, return JSON)
--------------------------------------
POST /narratives/<vis_type>/check-similar/     similarity check + query logging
POST /narratives/<vis_type>/generate/          REASONING + real DB charts → save draft
POST /narratives/<vis_type>/add-chart/         NL → QUERY → insert chart block
POST /narratives/<vis_type>/delete-block/      remove block by uuid
POST /narratives/<vis_type>/reorder/           set new block order by uuid list
POST /narratives/<vis_type>/regen-chart/       re-run QUERY on block with new prompt
POST /narratives/<vis_type>/publish/           render files, set status=published
POST /narratives/<vis_type>/view/              increment view_count
POST /narratives/<vis_type>/reset-draft/       delete this specific narrative (by id in body)
"""

from __future__ import annotations

import json
import logging
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import unquote

from django.conf import settings
from django.db.models import Count
from django.http import (
    FileResponse, Http404, JsonResponse, HttpResponseRedirect
)
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from academic.models import PaperFigure
from core.config import get_role_model
from core.llm_client import call_llm
from core.taxonomy import resolve_vis_type, vis_type_to_slug
from repository.models import RepoArtifact
from tracing.models import DriftAnnotation, Narrative, NarrativeQuery, Trace
from tracing.publisher import publish as do_publish
from tracing.chart_query import generate_chart_from_nl
from ui.query_agent import execute_query

logger = logging.getLogger(__name__)


# ── Preset chart definitions ───────────────────────────────────────────────────

PRESET_CHARTS = [
    {
        "id": "drift_severity",
        "label": "Drift severity breakdown",
        "prompt": (
            "Show a stacked bar chart of drift severity (none / minor / major) "
            "across the three drift dimensions: encoding, interaction, and task. "
            "Use green for none, amber for minor, red for major."
        ),
    },
    {
        "id": "frequency_over_time",
        "label": "Frequency over time (academic)",
        "prompt": (
            "Show a bar chart of the number of IEEE VIS papers per year "
            "that contain this visualization type, from 1995 to 2025."
        ),
    },
    {
        "id": "academic_vs_repo",
        "label": "Academic vs repository distribution",
        "prompt": (
            "Show a side-by-side bar chart comparing: "
            "the count of academic figures classified as this type, "
            "versus the count of repository notebooks that implement it."
        ),
    },
    {
        "id": "publication_trend",
        "label": "Per-year publication trend",
        "prompt": (
            "Show a line chart of the cumulative number of IEEE VIS papers "
            "featuring this visualization type over time."
        ),
    },
    {
        "id": "top_libraries",
        "label": "Top libraries in matching notebooks",
        "prompt": (
            "Show a horizontal bar chart of the top visualization libraries "
            "used in repository notebooks that implement this chart type, "
            "ordered by frequency."
        ),
    },
]


# ── Internal helpers ───────────────────────────────────────────────────────────

def _new_uuid() -> str:
    return str(uuid.uuid4())


def _resolve_vis_type_or_404(vis_type: str) -> str:
    resolved = resolve_vis_type(unquote(vis_type))
    if resolved is None:
        raise Http404(f"No vis_type found for '{vis_type}'")
    return resolved


def _generate_chart_block(prompt: str, vis_type: str) -> dict:
    """
    Call the QUERY model to turn a natural-language chart description into
    a Plotly spec. Returns a chart block dict. On failure, returns a block
    with an empty spec and an error note.
    """
    system = (
        "You are a data visualisation engineer. "
        "Given a plain-language chart description and a vis_type context, "
        "return a JSON object with exactly two keys: "
        '"data" (Plotly trace array) and "layout" (Plotly layout object). '
        "Use a light theme: paper_bgcolor and plot_bgcolor transparent, "
        "font color #475569. "
        "If you cannot generate real data, use plausible illustrative numbers. "
        "Return ONLY the JSON object, no markdown fences, no explanation."
    )
    user_prompt = f"vis_type context: {vis_type}\n\nChart request: {prompt}"
    try:
        result = call_llm(
            role="QUERY",
            prompt=user_prompt,
            response_format="json",
            system_prompt=system,
        )
        if isinstance(result, dict) and ("data" in result or "layout" in result):
            plotly_spec = result
        else:
            plotly_spec = {"data": [], "layout": {}, "_error": "unexpected shape"}
    except Exception as exc:
        logger.error("_generate_chart_block: QUERY call failed: %s", exc)
        plotly_spec = {"data": [], "layout": {}, "_error": str(exc)}

    return {
        "uuid": _new_uuid(),
        "type": "chart",
        "prompt": prompt,
        "plotly_spec": plotly_spec,
    }


def _spread_by_year(paper_ids: list[int], paper_figs: dict) -> list[int]:
    """
    Re-order a list of paper_ids so that distinct publication years are visited
    in round-robin fashion (ascending year) rather than clustering at the most
    recent one.

    Within each year the papers retain their original confidence-desc order.
    Result: year_min_p1, year_min+1_p1, ..., year_max_p1, year_min_p2, ...
    so a budget of N drawn from the front always spans as many distinct years
    as possible before revisiting any year.
    """
    by_year: dict[int, list[int]] = {}
    for pid in paper_ids:
        year = paper_figs[pid][0].paper.year or 0
        by_year.setdefault(year, []).append(pid)

    sorted_years = sorted(by_year.keys())
    result: list[int] = []
    while any(by_year[y] for y in sorted_years):
        for y in sorted_years:
            if by_year[y]:
                result.append(by_year[y].pop(0))
    return result


def _select_figures(
    vis_type: str,
    breadth_target: int = 8,
    depth_cap: int = 4,
    total_target: int = 30,
) -> list:
    """
    Two-budget figure selection with paper diversity, era balance, and year spread.

    Step 1 — fetch & group: all figures for the vis_type are fetched ordered by
      confidence desc, then grouped by paper_id.

    Step 2 — era split: papers are divided into two buckets:
      old  — annotation_source == 'visimages_json'  (VisImages, pre-2019)
      new  — llm_classified  (VIS2023/2024/2025 + seed_doi)
      Without this split, ~2,800 LLM figures stored at confidence=1.0 crowd out
      VisImages papers via the year tiebreaker.

    Step 3 — year spread within each bucket: _spread_by_year() round-robins
      through distinct publication years so the top slots span many years rather
      than clustering at the most recent one.

    Step 4 — loose 2:1 merge: old and new buckets are merged at roughly 2 old
      per 1 new, reflecting the corpus skew (VisImages is ~6× larger for most
      types) while ensuring newer papers appear early enough to reach the seed
      phase.  Not a strict interleave — consecutive entries from the same era
      are expected and fine.

    Phase 1 — seed: allocate up to depth_cap figures from the top
      breadth_target papers in the merged order.
    Phase 2 — recruit: spend freed slots on new papers, up to depth_cap each.
    Phase 3 — overflow: top up existing allocations if budget remains.

    Returns figures in paper-rank order, figures within each paper sorted by
    confidence descending.
    """
    all_figs = list(
        PaperFigure.objects.filter(vis_type=vis_type, is_visualization=True)
        .select_related("paper")
        .order_by("-vis_type_confidence", "-paper__year")
    )
    if not all_figs:
        return []

    # Group by paper, preserving confidence-ranked insertion order
    paper_figs: dict[int, list] = {}
    paper_order: list[int] = []
    seen: set[int] = set()
    for fig in all_figs:
        pid = fig.paper_id
        if pid not in seen:
            seen.add(pid)
            paper_order.append(pid)
            paper_figs[pid] = []
        paper_figs[pid].append(fig)

    # Split into era buckets
    old_papers: list[int] = []
    new_papers: list[int] = []
    for pid in paper_order:
        if paper_figs[pid][0].annotation_source == "visimages_json":
            old_papers.append(pid)
        else:
            new_papers.append(pid)

    # Year-spread within each bucket
    old_spread = _spread_by_year(old_papers, paper_figs)
    new_spread = _spread_by_year(new_papers, paper_figs)

    # Loose 2:1 merge (2 old, 1 new, repeat).  Not a strict interleave —
    # consecutive same-era entries are intentional and fine.
    ranked: list[int] = []
    oi = ni = 0
    while oi < len(old_spread) or ni < len(new_spread):
        for _ in range(2):
            if oi < len(old_spread):
                ranked.append(old_spread[oi]); oi += 1
        if ni < len(new_spread):
            ranked.append(new_spread[ni]); ni += 1

    # Phase 1 — seed top breadth_target papers
    selected: dict[int, list] = {}
    for pid in ranked[:breadth_target]:
        selected[pid] = paper_figs[pid][:depth_cap]

    remaining = total_target - sum(len(v) for v in selected.values())

    # Phase 2 — recruit new papers with freed slots
    for pid in ranked[breadth_target:]:
        if remaining <= 0:
            break
        take = min(depth_cap, remaining)
        figs = paper_figs[pid][:take]
        if figs:
            selected[pid] = figs
            remaining -= len(figs)

    # Phase 3 — overflow: top up existing papers beyond depth_cap
    for pid in ranked:
        if remaining <= 0:
            break
        if pid not in selected:
            continue
        current = len(selected[pid])
        extra = paper_figs[pid][current : current + remaining]
        if extra:
            selected[pid].extend(extra)
            remaining -= len(extra)

    # Return in paper-rank order
    result: list = []
    for pid in ranked:
        if pid in selected:
            result.extend(selected[pid])
    return result


def _select_artifacts(
    vis_type: str,
    guarantee: int = 3,
    total_cap: int = 24,
) -> list:
    """
    Platform-aware artifact selection with guaranteed minimum slots.

    For each platform that has at least one artifact matching vis_type,
    reserve up to ``guarantee`` slots (highest-starred first).  After
    all three platforms have been served, fill the remaining capacity
    (``total_cap`` minus the guaranteed count) from the full pool ranked
    by stars, skipping artifacts already selected.

    This prevents low-star platforms such as Observable HQ (max ~26
    likes) from being completely displaced by high-star Kaggle notebooks
    when both have relevant implementations for a vis_type.

    Platform order for guarantee pass: Kaggle → GitHub → ObservableHQ.
    The fill pass is pure stars-descending across all platforms.

    Constants summary (defaults):
      guarantee   = 3   guaranteed slots per platform that has ≥1 artifact
      total_cap   = 24  hard ceiling on the returned list
      fill slots  = total_cap − (platforms_represented × min(guarantee, available))
    """
    qs = (
        RepoArtifact.objects.filter(
            detected_chart_types__icontains=f'"{vis_type}"'
        )
        .select_related("source")
    )

    # Phase 1 — guaranteed slots per platform (in a predictable order so the
    # notebook block always shows platforms in the same sequence)
    selected: list = []
    selected_ids: set[int] = set()

    for platform in ("kaggle", "github", "observablehq"):
        for artifact in qs.filter(source__platform=platform).order_by("-source__stars")[:guarantee]:
            if artifact.id not in selected_ids:
                selected.append(artifact)
                selected_ids.add(artifact.id)

    # Phase 2 — fill remaining capacity, stars-ranked across all platforms
    remaining = total_cap - len(selected)
    if remaining > 0:
        for artifact in qs.order_by("-source__stars")[:total_cap]:
            if remaining <= 0:
                break
            if artifact.id not in selected_ids:
                selected.append(artifact)
                selected_ids.add(artifact.id)
                remaining -= 1

    return selected


def _get_traced_figures(vis_type: str) -> list:
    """
    Return PaperFigure objects that appear in annotated (Gemini-verified) traces
    for this vis_type, ordered by year descending.  Used by both generate() and
    update_figures() to populate the traced_papers tier of the figures block.
    """
    traced_ids = set(
        Trace.objects.filter(
            figure__vis_type=vis_type,
            annotation_status="annotated",
        ).values_list("figure_id", flat=True).distinct()
    )
    if not traced_ids:
        return []
    return list(
        PaperFigure.objects.filter(id__in=traced_ids)
        .select_related("paper")
        .order_by("-paper__year", "id")
    )


def _gather_evidence(vis_type: str) -> dict:
    """
    Pull evidence for a vis_type:
      figures    — up to 30 PaperFigures, selected by _select_figures
      artifacts  — up to 24 RepoArtifacts, platform-balanced by _select_artifacts
                   (at least 3 per platform that has notebooks, remainder by stars)
      drift_annotations — all DriftAnnotation records for verified traces

    Returns a dict with keys: figures, artifacts, drift_annotations,
    encoding_counts, interaction_counts, task_counts, drift_notes_sample.
    """
    figures = _select_figures(vis_type)

    artifacts = _select_artifacts(vis_type)

    drift_qs = list(
        DriftAnnotation.objects.filter(
            trace__figure__vis_type=vis_type,
            trace__verified=True,
        ).select_related("trace")
    )

    enc_counts = dict(Counter(da.encoding_drift for da in drift_qs))
    inter_counts = dict(Counter(da.interaction_drift for da in drift_qs))
    task_counts = dict(Counter(da.task_drift for da in drift_qs))

    notes_sample = [
        {
            "encoding_notes": da.encoding_notes,
            "interaction_notes": da.interaction_notes,
            "task_notes": da.task_notes,
        }
        for da in drift_qs[:3]
    ]

    return {
        "figures": figures,
        "artifacts": artifacts,
        "drift_annotations": drift_qs,
        "encoding_counts": enc_counts,
        "interaction_counts": inter_counts,
        "task_counts": task_counts,
        "drift_notes_sample": notes_sample,
    }


def _build_figures_block(figures: list, traced_figures: list | None = None) -> dict:
    """
    Build a figures block with two tiers:
      traced_papers — figures from annotated traces (Gemini-verified evidence)
      papers        — broader related pool for this vis_type

    Both use the grouped-by-paper schema. The block carries both so the
    publisher and author preview can render them as distinct subsections.
    """
    def _group_by_paper(figs: list) -> list:
        paper_groups: dict[int, dict] = {}
        paper_order: list[int] = []
        for f in figs:
            pid = f.paper_id
            if pid not in paper_groups:
                paper_groups[pid] = {
                    "paper_id": pid,
                    "title": f.paper.title,
                    "year": f.paper.year,
                    "doi": f.paper.doi or "",
                    "figures": [],
                }
                paper_order.append(pid)
            paper_groups[pid]["figures"].append({
                "id": f.id,
                "vis_type": f.vis_type,
                "image_local_path": f.image_local_path,
            })
        return [paper_groups[pid] for pid in paper_order]

    traced = traced_figures or []

    return {
        "uuid": _new_uuid(),
        "type": "figures",
        "vis_type": figures[0].vis_type if figures else (traced[0].vis_type if traced else ""),
        "figure_ids": [f.id for f in figures],
        # Legacy flat list — kept for backward compat with update_figures endpoint
        "metadata": [
            {
                "id": f.id,
                "title": f.paper.title,
                "year": f.paper.year,
                "doi": f.paper.doi or "",
                "vis_type": f.vis_type,
                "image_local_path": f.image_local_path,
            }
            for f in figures
        ],
        # Grouped-by-paper: traced evidence (annotated traces) shown first
        "traced_papers": _group_by_paper(traced),
        # Broader related pool — other figures for this vis_type
        "papers": _group_by_paper(figures),
    }


def _build_notebooks_block(artifacts: list, traced_artifacts: list | None = None) -> dict:
    """
    Build a notebooks block with two tiers:
      traced_notebooks — artifacts from annotated traces (Gemini-verified evidence)
      notebooks        — broader related pool for this vis_type
    """
    def _serialize(arts: list) -> list:
        return [
            {
                "id": a.id,
                "platform": a.source.platform,
                "title": a.source.title,
                "url": a.source.url,
                "stars": a.source.stars,
                "chart_types": a.get_detected_chart_types(),
            }
            for a in arts
        ]

    traced = traced_artifacts or []

    return {
        "uuid": _new_uuid(),
        "type": "notebooks",
        "artifact_ids": [a.id for a in artifacts],
        "traced_notebooks": _serialize(traced),
        "metadata": _serialize(artifacts),
    }


def _build_drift_evidence_block(vis_type: str) -> dict:
    """
    Snapshot all DriftAnnotation records for vis_type into a self-contained block.
    Groups annotations by dimension (encoding / interaction / task) then by
    severity (major → minor → none) so the UI can render a scrollable accordion.
    """
    annotations = list(
        DriftAnnotation.objects.filter(
            trace__figure__vis_type=vis_type,
            trace__verified=True,
        ).select_related("trace__figure__paper", "trace__artifact__source")
    )

    dimensions: dict[str, dict[str, list]] = {
        "encoding":    {"major": [], "minor": [], "none": []},
        "interaction": {"major": [], "minor": [], "none": []},
        "task":        {"major": [], "minor": [], "none": []},
    }

    for da in annotations:
        base = {
            "paper_title":    da.trace.figure.paper.title,
            "paper_year":     da.trace.figure.paper.year,
            "notebook_title": da.trace.artifact.source.title,
            "notebook_url":   da.trace.artifact.source.url,
            "platform":       da.trace.artifact.source.platform,
        }
        dimensions["encoding"][da.encoding_drift].append(
            dict(base, notes=da.encoding_notes or "")
        )
        dimensions["interaction"][da.interaction_drift].append(
            dict(base, notes=da.interaction_notes or "")
        )
        dimensions["task"][da.task_drift].append(
            dict(base, notes=da.task_notes or "")
        )

    totals = {
        dim: {sev: len(entries) for sev, entries in sev_map.items()}
        for dim, sev_map in dimensions.items()
    }

    return {
        "uuid":       _new_uuid(),
        "type":       "drift_evidence",
        "vis_type":   vis_type,
        "total":      len(annotations),
        "totals":     totals,
        "dimensions": dimensions,
    }


def _similarity_score(a: str, b: str) -> float:
    """
    Lightweight text-overlap similarity. Returns 0.0–1.0.
    Uses Jaccard overlap on lowercased word sets.
    Falls back to 0 on empty inputs.
    """
    if not a or not b:
        return 0.0
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    intersection = len(wa & wb)
    union = len(wa | wb)
    return intersection / union if union else 0.0



# ── Narrative lookup helper ────────────────────────────────────────────────────

def _get_narrative_from_body(vis_type: str, body: dict):
    """
    Resolve which Narrative to operate on.

    When the POST body contains a "narrative_id" key the narrative is fetched
    by primary key (scoped to vis_type for safety).  If no id is supplied the
    most recently generated narrative for that vis_type is used as a fallback
    so that the author page still works when a narrative was just created.

    Raises Http404 when no matching narrative exists.
    """
    narrative_id = body.get("narrative_id")
    if narrative_id:
        return get_object_or_404(Narrative, pk=narrative_id, vis_type=vis_type)
    narrative = (
        Narrative.objects.filter(vis_type=vis_type)
        .order_by("-generated_at")
        .first()
    )
    if narrative is None:
        raise Http404(f"No narrative found for vis_type '{vis_type}'")
    return narrative


# ── Real-data preset chart builder ────────────────────────────────────────────

def _base_layout(**kwargs) -> dict:
    base = {
        "paper_bgcolor": "rgba(248,250,252,0)",
        "plot_bgcolor": "rgba(248,250,252,0)",
        "font": {"color": "#475569", "family": "'JetBrains Mono',monospace", "size": 12},
        "margin": {"l": 50, "r": 20, "t": 40, "b": 50},
        "xaxis": {"gridcolor": "#e2e8f0", "tickfont": {"color": "#475569"}},
        "yaxis": {"gridcolor": "#e2e8f0", "tickfont": {"color": "#475569"}},
    }
    base.update(kwargs)
    return base


def build_preset_chart_specs(vis_type: str) -> dict[str, dict]:
    """
    Compute all 5 preset chart Plotly specs from the real database.
    Returns dict: preset_id → {"data": [...], "layout": {...}}
    No LLM involved — purely ORM queries.
    """
    specs: dict[str, dict] = {}

    # ── 1. Drift severity breakdown ────────────────────────────────────────────
    drift_qs = list(DriftAnnotation.objects.filter(
        trace__figure__vis_type=vis_type,
        trace__verified=True,
    ))
    enc   = Counter(da.encoding_drift for da in drift_qs)
    inter = Counter(da.interaction_drift for da in drift_qs)
    task  = Counter(da.task_drift for da in drift_qs)

    specs["drift_severity"] = {
        "data": [
            {
                "type": "bar", "name": "None",
                "x": ["Encoding", "Interaction", "Task"],
                "y": [enc.get("none", 0), inter.get("none", 0), task.get("none", 0)],
                "marker": {"color": "#0f766e"}, "opacity": 0.85,
            },
            {
                "type": "bar", "name": "Minor",
                "x": ["Encoding", "Interaction", "Task"],
                "y": [enc.get("minor", 0), inter.get("minor", 0), task.get("minor", 0)],
                "marker": {"color": "#d97706"}, "opacity": 0.85,
            },
            {
                "type": "bar", "name": "Major",
                "x": ["Encoding", "Interaction", "Task"],
                "y": [enc.get("major", 0), inter.get("major", 0), task.get("major", 0)],
                "marker": {"color": "#dc2626"}, "opacity": 0.85,
            },
        ],
        "layout": _base_layout(
            barmode="stack",
            title={"text": f"Drift Severity — {vis_type}", "font": {"size": 12, "color": "#475569"}},
            legend={"orientation": "h", "y": -0.25},
        ),
    }

    # ── 2. Frequency over time ─────────────────────────────────────────────────
    year_rows = list(
        PaperFigure.objects.filter(vis_type=vis_type, is_visualization=True)
        .exclude(paper__year=None)
        .values("paper__year")
        .annotate(count=Count("id"))
        .order_by("paper__year")
    )
    years  = [r["paper__year"] for r in year_rows]
    counts = [r["count"] for r in year_rows]

    specs["frequency_over_time"] = {
        "data": [{
            "type": "bar",
            "x": years, "y": counts,
            "marker": {"color": "#0369a1"}, "opacity": 0.85,
        }],
        "layout": _base_layout(
            title={"text": f"IEEE VIS Papers per Year — {vis_type}", "font": {"size": 12, "color": "#475569"}},
            xaxis={"title": "Year"},
            yaxis={"title": "Figure count"},
        ),
    }

    # ── 3. Academic vs repository ──────────────────────────────────────────────
    fig_count      = PaperFigure.objects.filter(vis_type=vis_type, is_visualization=True).count()
    artifact_count = RepoArtifact.objects.filter(
        detected_chart_types__icontains=f'"{vis_type}"'
    ).count()

    specs["academic_vs_repo"] = {
        "data": [{
            "type": "bar",
            "x": ["Academic Figures", "Repository Notebooks"],
            "y": [fig_count, artifact_count],
            "marker": {"color": ["#0369a1", "#7c3aed"]},
            "opacity": 0.85,
        }],
        "layout": _base_layout(
            title={"text": f"Academic vs Repository — {vis_type}", "font": {"size": 12, "color": "#475569"}},
            showlegend=False,
        ),
    }

    # ── 4. Cumulative publication trend ───────────────────────────────────────
    if years:
        cumulative, running = [], 0
        for c in counts:
            running += c
            cumulative.append(running)
        specs["publication_trend"] = {
            "data": [{
                "type": "scatter", "mode": "lines+markers",
                "x": years, "y": cumulative,
                "line": {"color": "#0f766e", "width": 2},
                "marker": {"color": "#0f766e", "size": 4},
            }],
            "layout": _base_layout(
                title={"text": f"Cumulative Papers — {vis_type}", "font": {"size": 12, "color": "#475569"}},
                xaxis={"title": "Year"},
                yaxis={"title": "Cumulative figure count"},
            ),
        }
    else:
        specs["publication_trend"] = {"data": [], "layout": _base_layout()}

    # ── 5. Top libraries in matching notebooks ─────────────────────────────────
    artifacts = list(
        RepoArtifact.objects.filter(
            detected_chart_types__icontains=f'"{vis_type}"'
        ).exclude(detected_libraries__in=["[]", ""])
    )
    lib_counts: dict[str, int] = {}
    for a in artifacts:
        for lib in a.get_detected_libraries():
            lib_counts[lib] = lib_counts.get(lib, 0) + 1

    top_libs = sorted(lib_counts.items(), key=lambda x: -x[1])[:10]
    if top_libs:
        lib_names = [t[0] for t in reversed(top_libs)]  # reversed for horizontal bar readability
        lib_vals  = [t[1] for t in reversed(top_libs)]
        specs["top_libraries"] = {
            "data": [{
                "type": "bar", "orientation": "h",
                "x": lib_vals, "y": lib_names,
                "marker": {"color": "#7c3aed"}, "opacity": 0.85,
            }],
            "layout": _base_layout(
                title={"text": f"Top Libraries — {vis_type}", "font": {"size": 12, "color": "#475569"}},
                xaxis={"title": "Notebook count"},
                height=320,
            ),
        }
    else:
        specs["top_libraries"] = {"data": [], "layout": _base_layout()}

    return specs


# ── Page views ─────────────────────────────────────────────────────────────────

def gallery(request):
    published = list(
        Narrative.objects.filter(status="published")
        .order_by("-view_count", "-published_at")
    )

    # Count how many published narratives exist per vis_type so the gallery
    # can show a version indicator when more than one exists.
    version_counts: dict[str, int] = {}
    for n in published:
        version_counts[n.vis_type] = version_counts.get(n.vis_type, 0) + 1

    narrative_cards = []
    for n in published:
        drift_qs = DriftAnnotation.objects.filter(
            trace__figure__vis_type=n.vis_type,
            trace__verified=True,
        )
        enc   = dict(Counter(da.encoding_drift for da in drift_qs))
        inter = dict(Counter(da.interaction_drift for da in drift_qs))
        task  = dict(Counter(da.task_drift for da in drift_qs))
        narrative_cards.append({
            "narrative": n,
            "excerpt": n.get_text_excerpt(),
            "trace_count": len(n.get_source_figures()),
            "drift": {"encoding": enc, "interaction": inter, "task": task},
            "version_count": version_counts.get(n.vis_type, 1),
        })

    from core.taxonomy import VIS_TYPES
    context = {
        "narrative_cards": narrative_cards,
        "all_vis_types": VIS_TYPES,
        "total_queries": NarrativeQuery.objects.count(),
        "total_published": len(published),
    }
    return render(request, "narratives/gallery.html", context)


def detail(request, vis_type: str, narrative_id: int | None = None):
    vis_type = _resolve_vis_type_or_404(vis_type)

    if narrative_id is not None:
        # Specific narrative requested by id
        narrative = get_object_or_404(
            Narrative, pk=narrative_id, vis_type=vis_type, status="published"
        )
    else:
        # Most recently published for this vis_type
        narrative = (
            Narrative.objects.filter(vis_type=vis_type, status="published")
            .order_by("-published_at")
            .first()
        )
        if narrative is None:
            return redirect("narratives:author", vis_type=vis_type_to_slug(vis_type))

    html_path = Path(settings.MEDIA_ROOT) / narrative.html_path
    if not html_path.exists():
        return redirect("narratives:author", vis_type=vis_type_to_slug(vis_type))

    # Increment view_count via a separate atomic update
    from django.db.models import F
    Narrative.objects.filter(pk=narrative.pk).update(view_count=F("view_count") + 1)

    return FileResponse(open(html_path, "rb"), content_type="text/html; charset=utf-8")


def author(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)

    # ?new=1 forces a blank slate regardless of existing narratives.
    # Used by the "New Narrative" button in the gallery.
    force_new = request.GET.get("new") == "1"

    if force_new:
        narrative = None
    else:
        # ?narrative_id= loads a specific narrative (used by gallery EDIT links)
        narrative_id_param = request.GET.get("narrative_id")
        if narrative_id_param:
            try:
                narrative = Narrative.objects.get(pk=int(narrative_id_param), vis_type=vis_type)
            except (Narrative.DoesNotExist, ValueError):
                narrative = None
        else:
            # Most recently generated narrative for this vis_type (any status)
            narrative = (
                Narrative.objects.filter(vis_type=vis_type)
                .order_by("-generated_at")
                .first()
            )

    # All narratives for this type — used to display the draft/version count
    all_narratives = list(
        Narrative.objects.filter(vis_type=vis_type).order_by("-generated_at")
    )
    draft_count = len(all_narratives)

    # Prior query count for this type
    prior_count = NarrativeQuery.objects.filter(vis_type=vis_type).count()

    from core.taxonomy import TAXONOMY, VIS_TYPES
    vis_type_groups = [
        {"category": category, "types": subtypes}
        for category, subtypes in TAXONOMY.items()
    ]
    context = {
        "vis_type": vis_type,
        "vis_type_json": json.dumps(vis_type),
        "narrative": narrative,
        "narrative_id": narrative.id if narrative else None,
        "narrative_id_json": json.dumps(narrative.id if narrative else None),
        "draft_count": draft_count,
        "all_narratives": all_narratives,
        "prior_query_count": prior_count,
        "preset_charts": PRESET_CHARTS,
        "preset_charts_json": json.dumps(PRESET_CHARTS),
        "all_vis_types": VIS_TYPES,
        "vis_type_groups_json": json.dumps(vis_type_groups),
        "is_blank_new": force_new,
    }
    return render(request, "narratives/author.html", context)


def author_new(request):
    """
    Blank-slate author page with no vis_type pre-selected.
    Reached via the gallery "+ NEW NARRATIVE" button.
    The user picks a vis_type in the panel; generating creates a new row.
    """
    from core.taxonomy import TAXONOMY, VIS_TYPES
    vis_type_groups = [
        {"category": category, "types": subtypes}
        for category, subtypes in TAXONOMY.items()
    ]
    context = {
        "vis_type": "",
        "vis_type_json": json.dumps(""),
        "narrative": None,
        "narrative_id": None,
        "narrative_id_json": "null",
        "draft_count": 0,
        "all_narratives": [],
        "prior_query_count": 0,
        "preset_charts": PRESET_CHARTS,
        "preset_charts_json": json.dumps(PRESET_CHARTS),
        "all_vis_types": VIS_TYPES,
        "vis_type_groups_json": json.dumps(vis_type_groups),
        "is_blank_new": True,
    }
    return render(request, "narratives/author.html", context)



# ── API endpoints ──────────────────────────────────────────────────────────────

@csrf_exempt
@require_POST
def check_similar(request, vis_type: str):
    """
    1. Log the query to NarrativeQuery immediately.
    2. Fetch prior queries for this vis_type.
    3. Score similarity (lightweight text overlap).
    4. Return top matches.
    """
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    query_text = body.get("query_text", "").strip()
    if not query_text:
        return JsonResponse({"error": "query_text required"}, status=400)

    # Log interest (even if user never generates)
    NarrativeQuery.objects.create(vis_type=vis_type, query_text=query_text)

    # Gather prior queries (excluding the one just logged)
    prior_queries = list(
        NarrativeQuery.objects.filter(vis_type=vis_type)
        .exclude(query_text=query_text)
        .order_by("-timestamp")[:100]
    )

    # Group identical/near-duplicate queries
    grouped: dict[str, dict] = {}
    for pq in prior_queries:
        score = _similarity_score(query_text, pq.query_text)
        if score >= 0.25:
            key = pq.query_text[:120]
            if key not in grouped:
                grouped[key] = {
                    "query_text": pq.query_text,
                    "count": 0,
                    "score": score,
                    "narrative_id": pq.narrative_id,
                    "narrative_vis_type": pq.narrative.vis_type if pq.narrative else None,
                }
            grouped[key]["count"] += 1

    matches = sorted(grouped.values(), key=lambda x: (-x["score"], -x["count"]))[:5]

    # Published narrative for this type (for linking)
    published = Narrative.objects.filter(vis_type=vis_type, status="published").first()

    return JsonResponse({
        "matches": matches,
        "total_prior_queries": NarrativeQuery.objects.filter(vis_type=vis_type).count(),
        "published_narrative_id": published.id if published else None,
    })


@csrf_exempt
@require_POST
def generate(request, vis_type: str):
    """
    Generate a new draft narrative:
    1. Gather evidence (figures, artifacts, drift)
    2. Call REASONING → text block
    3. Call QUERY for each enabled preset + free NL extras → chart blocks
    4. Assemble figures + notebooks blocks
    5. Save Narrative (upsert by vis_type)
    """
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    query_text = body.get("query_text", "").strip()
    enabled_presets = body.get("enabled_presets", [p["id"] for p in PRESET_CHARTS])
    extra_chart_prompts = body.get("extra_chart_prompts", [])

    evidence = _gather_evidence(vis_type)
    figures = evidence["figures"]
    artifacts = evidence["artifacts"]

    # ── 1. REASONING → narrative text ─────────────────────────────────────────
    from core.prompts.generate_narrative import (
        GENERATE_NARRATIVE_SYSTEM_PROMPT,
        generate_narrative_prompt,
    )

    # Build aggregate corpus context from all figures for this vis_type.
    # Collect unique papers, then compute: total count, year range, track
    # distribution, top keywords, and a small year-spread abstract sample.
    _all_figs = list(
        PaperFigure.objects.filter(vis_type=vis_type, is_visualization=True)
        .select_related("paper")
        .order_by("-paper__year")
    )
    _paper_seen: set[int] = set()
    _papers_ordered: list = []
    for _f in _all_figs:
        if _f.paper_id not in _paper_seen:
            _paper_seen.add(_f.paper_id)
            _papers_ordered.append(_f.paper)

    _total_papers = len(_papers_ordered)
    _years = [p.year for p in _papers_ordered if p.year]
    _year_range = (min(_years), max(_years)) if _years else (0, 0)

    _track_counts = dict(Counter(p.track for p in _papers_ordered if p.track))

    # Top keywords — flatten and count across all papers
    _kw_counter: Counter = Counter()
    for _p in _papers_ordered:
        _kw_counter.update(_p.get_keywords())
    _top_keywords = [kw for kw, _ in _kw_counter.most_common(10)]

    # Abstract sample — pick up to 3 papers spread across the year range
    # that have a non-empty abstract, preferring distinct eras.
    _with_abstract = [p for p in _papers_ordered if p.abstract and p.abstract.strip()]
    _abstract_sample: list[dict] = []
    if _with_abstract:
        yr_lo, yr_hi = _year_range
        span = max(yr_hi - yr_lo, 1)
        _buckets: list[list] = [[], [], []]
        for _p in _with_abstract:
            if _p.year:
                slot = min(int((_p.year - yr_lo) / span * 3), 2)
                _buckets[slot].append(_p)
        for _bucket in _buckets:
            if _bucket and len(_abstract_sample) < 3:
                _pick = max(_bucket, key=lambda p: len(p.abstract))
                _abstract_sample.append({
                    "title":    _pick.title,
                    "year":     _pick.year,
                    "track":    _pick.track,
                    "abstract": _pick.abstract[:600],
                })

    narrative_prompt = generate_narrative_prompt(
        vis_type=vis_type,
        total_papers=_total_papers,
        year_range=_year_range,
        tracks=_track_counts,
        top_keywords=_top_keywords,
        abstract_sample=_abstract_sample,
        total_traces=len(evidence["drift_annotations"]),
        encoding_drift_counts=evidence["encoding_counts"],
        interaction_drift_counts=evidence["interaction_counts"],
        task_drift_counts=evidence["task_counts"],
        drift_notes_sample=evidence["drift_notes_sample"],
    )

    # Append the user's focus prompt if provided
    if query_text:
        narrative_prompt += f"\n\nAuthor's focus: {query_text}"

    try:
        narrative_text = str(call_llm(
            role="REASONING",
            prompt=narrative_prompt,
            system_prompt=GENERATE_NARRATIVE_SYSTEM_PROMPT,
        ))
    except Exception as exc:
        logger.error("generate: REASONING call failed for %s: %s", vis_type, exc)
        narrative_text = (
            f"[Generation failed: {exc}]\n\n"
            f"This narrative for {vis_type} could not be generated. "
            f"Please check your API keys and rate limits."
        )

    text_block = {
        "uuid": _new_uuid(),
        "type": "text",
        "content": narrative_text,
    }

    # ── 1b. Guidance prompt block (optional) ──────────────────────────────────
    # Inserted immediately after the text block so the author can see—and
    # optionally delete—the focus prompt before publishing.
    prompt_blocks = []
    if query_text:
        prompt_blocks.append({
            "uuid": _new_uuid(),
            "type": "query_prompt",
            "content": query_text,
        })

    # ── 2. Charts: presets from real DB data, extras via QUERY model ──────────
    chart_blocks = []
    preset_map = {p["id"]: p for p in PRESET_CHARTS}

    # Compute all preset specs from the real database (no LLM, no fake numbers)
    preset_specs = build_preset_chart_specs(vis_type)

    for preset_id in enabled_presets:
        if preset_id in preset_map and preset_id in preset_specs:
            chart_blocks.append({
                "uuid": _new_uuid(),
                "type": "chart",
                "prompt": preset_map[preset_id]["label"],   # human-readable label as caption
                "plotly_spec": preset_specs[preset_id],
            })

    # Extra / custom charts go through the Text-to-SQL → Plotly pipeline
    for extra_prompt in extra_chart_prompts:
        if extra_prompt.strip():
            chart_blocks.append(generate_chart_from_nl(extra_prompt.strip(), vis_type))

    # ── 3. Evidence blocks ─────────────────────────────────────────────────────
    traced_figs = _get_traced_figures(vis_type)
    traced_artifact_ids = set(
        Trace.objects.filter(
            figure__vis_type=vis_type,
            annotation_status="annotated",
        ).values_list("artifact_id", flat=True).distinct()
    )

    traced_arts_qs = (
        RepoArtifact.objects.filter(id__in=traced_artifact_ids)
        .select_related("source")
        .order_by("-source__stars")
    ) if traced_artifact_ids else []

    figures_block   = _build_figures_block(figures, traced_figures=traced_figs)
    notebooks_block = _build_notebooks_block(artifacts, traced_artifacts=list(traced_arts_qs))
    drift_evidence_block = _build_drift_evidence_block(vis_type)

    all_blocks = prompt_blocks + [text_block] + chart_blocks + [drift_evidence_block, figures_block, notebooks_block]

    # ── 4. Create new Narrative row ───────────────────────────────────────────
    model_used = get_role_model("REASONING")

    narrative = Narrative.objects.create(
        vis_type=vis_type,
        status="draft",
        blocks=json.dumps(all_blocks),
        query_text=query_text,
        source_figures=json.dumps([f.id for f in figures]),
        source_artifacts=json.dumps([a.id for a in artifacts]),
        model_used=model_used,
    )

    # Link the single most recent unlinked NarrativeQuery for this vis_type
    # to the new narrative.  This is the query that was logged by check_similar
    # immediately before the author clicked Generate.
    recent_query = (
        NarrativeQuery.objects.filter(vis_type=vis_type, narrative__isnull=True)
        .order_by("-timestamp")
        .first()
    )
    if recent_query:
        recent_query.narrative = narrative
        recent_query.save(update_fields=["narrative"])

    return JsonResponse({
        "narrative_id": narrative.id,
        "blocks": all_blocks,
        "vis_type": vis_type,
    })


@csrf_exempt
@require_POST
def add_chart(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    prompt = body.get("prompt", "").strip()
    if not prompt:
        return JsonResponse({"error": "prompt required"}, status=400)

    narrative = _get_narrative_from_body(vis_type, body)
    block = generate_chart_from_nl(prompt, vis_type)
    blocks = narrative.get_blocks()
    # Insert before the figures/notebooks blocks (keep evidence last)
    insert_at = next(
        (i for i, b in enumerate(blocks) if b.get("type") in ("figures", "notebooks")),
        len(blocks),
    )
    blocks.insert(insert_at, block)
    narrative.set_blocks(blocks)
    narrative.save(update_fields=["blocks"])

    return JsonResponse({"block": block})


@csrf_exempt
@require_POST
def delete_block(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    block_uuid = body.get("uuid", "").strip()
    if not block_uuid:
        return JsonResponse({"error": "uuid required"}, status=400)

    narrative = _get_narrative_from_body(vis_type, body)
    blocks = [b for b in narrative.get_blocks() if b.get("uuid") != block_uuid]
    narrative.set_blocks(blocks)
    narrative.save(update_fields=["blocks"])

    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def reorder(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    new_order = body.get("order", [])  # list of uuids
    narrative = _get_narrative_from_body(vis_type, body)
    blocks = narrative.get_blocks()
    block_map = {b["uuid"]: b for b in blocks if "uuid" in b}

    # Validate: same set of uuids
    if set(new_order) != set(block_map.keys()):
        return JsonResponse({"error": "uuid set mismatch"}, status=400)

    reordered = [block_map[uid] for uid in new_order if uid in block_map]
    narrative.set_blocks(reordered)
    narrative.save(update_fields=["blocks"])

    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def regen_chart(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    block_uuid = body.get("uuid", "").strip()
    new_prompt = body.get("new_prompt", "").strip()
    if not block_uuid:
        return JsonResponse({"error": "uuid required"}, status=400)

    narrative = _get_narrative_from_body(vis_type, body)
    blocks = narrative.get_blocks()

    updated_block = None
    for i, block in enumerate(blocks):
        if block.get("uuid") == block_uuid and block.get("type") in ("chart", "image"):
            prompt = new_prompt or block.get("prompt", "")
            new_block = generate_chart_from_nl(prompt, vis_type)
            new_block["uuid"] = block_uuid  # keep the same uuid
            blocks[i] = new_block
            updated_block = new_block
            break

    if updated_block is None:
        return JsonResponse({"error": "chart block not found"}, status=404)

    narrative.set_blocks(blocks)
    narrative.save(update_fields=["blocks"])

    return JsonResponse({"block": updated_block})


@csrf_exempt
@require_POST
def publish(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        body = {}

    narrative = _get_narrative_from_body(vis_type, body)

    try:
        paths = do_publish(narrative)
    except Exception as exc:
        logger.error("publish: failed for %s id=%s: %s", vis_type, narrative.pk, exc)
        return JsonResponse({"error": str(exc)}, status=500)

    narrative.status = "published"
    narrative.html_path = paths["html_path"]
    narrative.json_ld_path = paths["json_ld_path"]
    narrative.published_at = timezone.now()
    narrative.save(update_fields=["status", "html_path", "json_ld_path", "published_at"])

    # Link unlinked NarrativeQueries for this vis_type that were created before
    # this narrative was generated (i.e. they seeded this narrative).  We look
    # for queries whose timestamp predates the narrative's generated_at and that
    # have not yet been linked.
    NarrativeQuery.objects.filter(
        vis_type=vis_type,
        narrative__isnull=True,
        timestamp__lte=narrative.generated_at,
    ).update(narrative=narrative)

    return JsonResponse({
        "ok": True,
        "narrative_id": narrative.pk,
        "redirect": f"/narratives/{vis_type_to_slug(vis_type)}/{narrative.pk}/",
    })


@csrf_exempt
@require_POST
def increment_view(request, vis_type: str):
    vis_type = _resolve_vis_type_or_404(vis_type)
    from django.db.models import F
    try:
        body = json.loads(request.body)
        narrative_id = body.get("narrative_id")
    except Exception:
        narrative_id = None
    qs = Narrative.objects.filter(vis_type=vis_type, status="published")
    if narrative_id:
        qs = qs.filter(pk=narrative_id)
    qs.update(view_count=F("view_count") + 1)
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def reset_draft(request, vis_type: str):
    """
    Delete a specific Narrative row identified by narrative_id in the POST body.
    Also deletes any rendered files (HTML, JSON-LD) from disk.
    Returns {"ok": True}.
    """
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        body = {}

    narrative = _get_narrative_from_body(vis_type, body)

    # Delete rendered files from disk
    for rel_path in [narrative.html_path, narrative.json_ld_path, narrative.pdf_path]:
        if rel_path:
            try:
                (Path(settings.MEDIA_ROOT) / rel_path).unlink(missing_ok=True)
            except Exception as exc:
                logger.warning("reset_draft: could not delete %s: %s", rel_path, exc)

    narrative.delete()
    return JsonResponse({"ok": True})


@require_GET
def serve_jsonld(request, narrative_id: int):
    """
    Serve the JSON-LD sidecar for a published narrative by its primary key.
    Route: GET /narratives/jsonld/<narrative_id>/
    """
    narrative = get_object_or_404(Narrative, pk=narrative_id, status="published")

    jsonld_path = Path(settings.MEDIA_ROOT) / narrative.json_ld_path
    if not jsonld_path.exists():
        raise Http404("JSON-LD file missing — re-publish the narrative to regenerate it")

    return FileResponse(
        open(jsonld_path, "rb"),
        content_type="application/ld+json; charset=utf-8",
    )

@require_GET
def figures_pool(request, vis_type: str):
    """
    Return the full candidate pool of figures for a vis_type so the author
    page can render the figure picker.

    Uses _select_figures with a larger total_target so the pool is wide
    enough to show figures the automatic selection didn't include.

    Response shape:
      {
        "figures": [
          { "id": int, "image_path": str, "paper_title": str,
            "paper_year": int|null, "paper_doi": str, "annotation_source": str }
          ...
        ]
      }
    """
    vis_type = _resolve_vis_type_or_404(vis_type)
    pool = _select_figures(vis_type, breadth_target=20, depth_cap=6, total_target=80)
    return JsonResponse({
        "figures": [
            {
                "id": f.id,
                "image_path": f.image_local_path,
                "paper_title": f.paper.title,
                "paper_year": f.paper.year,
                "paper_doi": f.paper.doi or "",
                "annotation_source": f.annotation_source,
            }
            for f in pool
        ]
    })


@csrf_exempt
@require_POST
def update_figures(request, vis_type: str):
    """
    Replace the figures block in a narrative with a user-specified set.

    POST body:
      { "narrative_id": int, "figure_ids": [int, ...] }

    Fetches the requested PaperFigures from the DB (in the supplied order),
    rebuilds the figures block preserving its existing UUID, and writes back
    both the block list and source_figures on the Narrative row so the
    gallery card count and JSON-LD sidecar stay consistent.

    Traced figures are filtered to the user-selected set for this narrative
    version; the underlying Trace rows remain untouched.

    Returns: { "block": <updated figures block dict> }
    """
    vis_type = _resolve_vis_type_or_404(vis_type)
    try:
        body = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)

    figure_ids = body.get("figure_ids", [])
    if not isinstance(figure_ids, list):
        return JsonResponse({"error": "figure_ids must be a list"}, status=400)

    narrative = _get_narrative_from_body(vis_type, body)

    # Fetch the requested figures, preserving the caller-supplied order
    fig_map = {
        f.id: f
        for f in PaperFigure.objects.filter(id__in=figure_ids).select_related("paper")
    }
    ordered_figs = [fig_map[fid] for fid in figure_ids if fid in fig_map]

    # Find the existing figures block to preserve its UUID
    blocks = narrative.get_blocks()
    existing_uuid = next(
        (b["uuid"] for b in blocks if b.get("type") == "figures"),
        _new_uuid(),
    )

    traced_selected_ids = set(
        Trace.objects.filter(
            figure_id__in=[f.id for f in ordered_figs],
            annotation_status="annotated",
        ).values_list("figure_id", flat=True).distinct()
    )
    traced_selected_figs = [f for f in ordered_figs if f.id in traced_selected_ids]

    new_block = _build_figures_block(ordered_figs, traced_figures=traced_selected_figs)
    new_block["uuid"] = existing_uuid  # stable UUID — block doesn't jump in the list

    # Replace the figures block in place, or append if somehow absent
    replaced = False
    for i, b in enumerate(blocks):
        if b.get("type") == "figures":
            blocks[i] = new_block
            replaced = True
            break
    if not replaced:
        blocks.append(new_block)

    narrative.set_blocks(blocks)
    # Keep source_figures in sync — used by gallery card count and JSON-LD sidecar
    narrative.source_figures = json.dumps([f.id for f in ordered_figs])
    narrative.save(update_fields=["blocks", "source_figures"])

    return JsonResponse({"block": new_block})
