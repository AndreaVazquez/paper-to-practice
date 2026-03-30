"""
UI views for the From Paper to Practice interface.

Views:
  /             — Dashboard: corpus statistics + Plotly charts
  /explore/<vis_type>/  — Explore by chart type
  /query/       — Natural language query
  /activity/    — Live pipeline monitor
  /activity/stream/     — SSE event stream
  /run/<command>/       — Trigger a pipeline management command
"""

import json
import logging
import queue
import re
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from urllib.parse import urlencode

from django.core.paginator import Paginator
from django.db.models import Count
from django.http import JsonResponse, StreamingHttpResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from academic.models import Paper, PaperFigure
from core.agent_log import emit, get_agent_status, subscribe, unsubscribe
from repository.models import RepoArtifact, RepoSource
from tracing.models import DriftAnnotation, Narrative, Trace

logger = logging.getLogger(__name__)


# ── Dashboard ──────────────────────────────────────────────────────────────────

INVALID_REASON_LABELS = {
    "workflow_diagram": "Workflow / system diagram",
    "map_like_flow": "Flow map / geographic path view",
    "topology_mesh": "Topology / mesh graph",
    "adjacent_chart": "Adjacent but different chart family",
    "scientific_figure": "Scientific / domain figure",
    "repository_mismatch": "Repository content unrelated",
    "other_nonmatching": "Other non-matching figure",
}

DRIFT_SEVERITY_LABELS = {
    "major": "Major",
    "minor": "Minor",
    "none": "None",
}

DRIFT_DIMENSION_ORDER = ["encoding", "interaction", "task"]
DRIFT_DIMENSIONS = {
    "encoding": {
        "label": "Encoding",
        "notes_field": "encoding_notes",
        "severity_field": "encoding_drift",
        "other_label": "Other encoding rationale",
        "themes": [
            {
                "id": "structure_substitution",
                "label": "Mark / structure substitution",
                "summary": (
                    "The repository implementation swaps the core visual grammar "
                    "or mark structure used in the paper figure for a different one."
                ),
                "patterns": [
                    r"\bstreamgraph\b",
                    r"\bthemeriver\b",
                    r"\bsankey\b",
                    r"\bhistogram\b",
                    r"\bbar chart\b",
                    r"\barea chart\b",
                    r"\bscatterplot\b",
                    r"\bridgeline\b",
                    r"\bheatmap\b",
                    r"\bnode-link\b",
                    r"\bbubble\b",
                    r"\bdot plot\b",
                    r"\bdendrogram\b",
                    r"\bnetwork\b",
                    r"\bflow\b",
                    r"\btree\b",
                    r"\bgraph\b",
                ],
            },
            {
                "id": "layout_composition",
                "label": "Layout / composition change",
                "summary": (
                    "Paper and repository stay near the same family, but the "
                    "layout, stacking, spatial arrangement, or composition changes."
                ),
                "patterns": [
                    r"\blayout\b",
                    r"\bstacked\b",
                    r"\bbaseline\b",
                    r"\bcentered\b",
                    r"\bwiggle\b",
                    r"\bgrouped\b",
                    r"\bsmall multiples\b",
                    r"\btreemap\b",
                    r"\bsunburst\b",
                    r"\bradial\b",
                    r"\bcircular\b",
                    r"\bmatrix\b",
                    r"\btabular\b",
                    r"\bpartitioned\b",
                    r"\bgrid\b",
                    r"\bhorizontal\b",
                    r"\bvertical\b",
                ],
            },
            {
                "id": "generic_simplification",
                "label": "Simplification to generic charts",
                "summary": (
                    "A custom or highly tailored academic encoding is flattened "
                    "into a more generic, static, or library-default chart."
                ),
                "patterns": [
                    r"\bsimplif",
                    r"\bgeneric\b",
                    r"\bbasic\b",
                    r"\bdefault\b",
                    r"\bstatic\b",
                    r"\bminimal\b",
                    r"\bcanonical\b",
                    r"\bstandard\b",
                    r"\bsimple\b",
                ],
            },
            {
                "id": "multi_view_context",
                "label": "Multi-view / interface context",
                "summary": (
                    "The paper figure depends on coordinated views, interface "
                    "scaffolding, or focus+context framing that the repository drops."
                ),
                "patterns": [
                    r"\bmulti-view\b",
                    r"\bmultiple-view\b",
                    r"\bcoordinated\b",
                    r"\bdashboard\b",
                    r"\binterface\b",
                    r"\bgui\b",
                    r"\bwindow\b",
                    r"\bfocus\+context\b",
                    r"\boverview\+detail\b",
                    r"\bbrushing\b",
                    r"\blinked\b",
                    r"\bcontrol panel\b",
                    r"\btoolbar\b",
                    r"\bpanel\b",
                ],
            },
            {
                "id": "annotation_overlay",
                "label": "Annotations / overlays removed",
                "summary": (
                    "The justification highlights missing annotations, guides, "
                    "legends, or overlays that materially shape the encoding."
                ),
                "patterns": [
                    r"\bannotation",
                    r"\bevent\b",
                    r"\blabel",
                    r"\boverlay",
                    r"\bguide",
                    r"\blegend",
                    r"\bslider\b",
                    r"\brange\b",
                    r"\boverview\b",
                    r"\bdetail\b",
                ],
            },
            {
                "id": "context_mismatch",
                "label": "Context / implementation mismatch",
                "summary": (
                    "The note indicates the repository content is teaching "
                    "material, unrelated code, or otherwise misaligned."
                ),
                "patterns": [
                    r"\bdifferent context\b",
                    r"\bentirely different\b",
                    r"\bunrelated\b",
                    r"\btutorial\b",
                    r"\bdoes not include\b",
                    r"\black\b",
                    r"\bcomplete disconnect\b",
                ],
            },
        ],
    },
    "interaction": {
        "label": "Interaction",
        "notes_field": "interaction_notes",
        "severity_field": "interaction_drift",
        "other_label": "Other interaction rationale",
        "themes": [
            {
                "id": "filter_reorder_loss",
                "label": "Filtering / reordering loss",
                "summary": (
                    "The academic design depends on interactive filtering, "
                    "reordering, grouping, or derivation that the repository omits."
                ),
                "patterns": [
                    r"\bfilter",
                    r"\breorder",
                    r"\bdrill-down\b",
                    r"\bgrouping\b",
                    r"\baggregation\b",
                    r"\bderive",
                    r"\bderivation\b",
                    r"\bmerge",
                    r"\bselection\b",
                ],
            },
            {
                "id": "navigation_coordination_loss",
                "label": "Navigation / coordination loss",
                "summary": (
                    "Linked views, navigation controls, or coordinated interaction "
                    "patterns are missing from the repository version."
                ),
                "patterns": [
                    r"\bzoom\b",
                    r"\bpan\b",
                    r"\bbrush",
                    r"\blinked\b",
                    r"\bnavigation\b",
                    r"\boverview\b",
                    r"\bdetail\b",
                    r"\bslider\b",
                    r"\bcoordinated\b",
                ],
            },
            {
                "id": "default_static_interaction",
                "label": "Reduced to basic or static interaction",
                "summary": (
                    "Specialized interaction is replaced by static output or "
                    "library-default behaviors such as tooltips."
                ),
                "patterns": [
                    r"\bstatic\b",
                    r"\bnon-interactive\b",
                    r"\bdefault\b",
                    r"\bbasic\b",
                    r"\btooltip",
                    r"\blibrary-default\b",
                    r"\bonly provides\b",
                    r"\bwithout interaction\b",
                    r"\babsent\b",
                ],
            },
            {
                "id": "system_capability_gap",
                "label": "Specialized interaction system missing",
                "summary": (
                    "The paper describes a richer interactive system or analytic "
                    "manipulation layer than the repository implementation supports."
                ),
                "patterns": [
                    r"\binteractive system\b",
                    r"\bsophisticated\b",
                    r"\bcomplex interactive\b",
                    r"\bcapabilities\b",
                    r"\bmanipulation\b",
                    r"\bdynamic\b",
                    r"\binteractivity\b",
                ],
            },
            {
                "id": "interaction_context_mismatch",
                "label": "Interaction context mismatch",
                "summary": (
                    "The note points to demo, gallery, or unrelated repository "
                    "material that is not trying to reproduce the paper workflow."
                ),
                "patterns": [
                    r"\btutorial\b",
                    r"\bgallery\b",
                    r"\bexample\b",
                    r"\bunrelated\b",
                    r"\bdifferent context\b",
                    r"\bstreaming updates\b",
                ],
            },
            {
                "id": "interaction_preserved",
                "label": "Interaction largely preserved",
                "summary": (
                    "The repository appears to retain most of the intended "
                    "interaction model described by the paper."
                ),
                "patterns": [
                    r"\bfaithfully reproduced\b",
                    r"\bpreserved\b",
                    r"\bretains\b",
                    r"\bconsistent with\b",
                    r"\bincludes interaction\b",
                ],
            },
        ],
    },
    "task": {
        "label": "Task",
        "notes_field": "task_notes",
        "severity_field": "task_drift",
        "other_label": "Other task rationale",
        "themes": [
            {
                "id": "exploration_to_reporting",
                "label": "Exploration to reporting shift",
                "summary": (
                    "The academic task is exploratory analysis, while the "
                    "repository turns the visualization into reporting or display."
                ),
                "patterns": [
                    r"\bexploratory\b",
                    r"\bexploration\b",
                    r"\breporting\b",
                    r"\bstatic report\b",
                    r"\bknowledge crystallization\b",
                    r"\banalytic goal\b",
                ],
            },
            {
                "id": "method_to_specific_use_case",
                "label": "Method to specific use case",
                "summary": (
                    "The paper proposes a general method or framework, while the "
                    "repository applies it to a narrow or specific use case."
                ),
                "patterns": [
                    r"\bgeneral framework\b",
                    r"\bgeneral-purpose\b",
                    r"\bmethodological\b",
                    r"\bformalization\b",
                    r"\bnovel visual analysis technique\b",
                    r"\bspecific diagnostic task\b",
                    r"\buse case\b",
                    r"\bscenario\b",
                ],
            },
            {
                "id": "domain_context_shift",
                "label": "Domain / dataset shift",
                "summary": (
                    "The task changes because the repository is grounded in a "
                    "different dataset, application domain, or workflow."
                ),
                "patterns": [
                    r"\bdataset\b",
                    r"\bdomain\b",
                    r"\bapplication\b",
                    r"\bsports\b",
                    r"\bbiological\b",
                    r"\bsimulation\b",
                    r"\bmachine learning\b",
                    r"\benergy flow\b",
                    r"\btool call\b",
                    r"\bprocess\b",
                ],
            },
            {
                "id": "demo_gallery_mismatch",
                "label": "Demo / gallery mismatch",
                "summary": (
                    "The repository note describes a demo, tutorial, or gallery "
                    "artifact rather than a faithful task reproduction."
                ),
                "patterns": [
                    r"\bgallery\b",
                    r"\btutorial\b",
                    r"\bdemo\b",
                    r"\bgeneric\b",
                    r"\beducational\b",
                    r"\bexample\b",
                ],
            },
            {
                "id": "task_preserved",
                "label": "Task largely preserved",
                "summary": (
                    "The repository appears to maintain the intended analytic or "
                    "communicative task of the academic design."
                ),
                "patterns": [
                    r"\bretains its intended purpose\b",
                    r"\bconsistent with\b",
                    r"\bfaithfully reproduced\b",
                    r"\bsame analytic goal\b",
                ],
            },
        ],
    },
}

INVALID_REASON_SUMMARIES = {
    "workflow_diagram": (
        "Most invalid cases come from workflow schematics, UI snapshots, or "
        "conceptual system diagrams that were classified as if they were charts."
    ),
    "map_like_flow": (
        "Most invalid cases are map-based or path-based views, where flow or "
        "movement is shown geographically instead of through the claimed chart grammar."
    ),
    "topology_mesh": (
        "Most invalid cases are graph, topology, or mesh depictions that look "
        "linked or structural, but do not follow the expected visualization form."
    ),
    "adjacent_chart": (
        "Most invalid cases are near misses: visually adjacent chart families "
        "that resemble the claimed type but use a different structure or encoding."
    ),
    "scientific_figure": (
        "Most invalid cases are scientific or domain-specific figures rather than "
        "the target visualization family tracked by the dashboard."
    ),
    "repository_mismatch": (
        "A recurring issue is that the linked repository content is unrelated to "
        "the figure, so the trace fails even when the paper-side image is visible."
    ),
    "other_nonmatching": (
        "Invalid traces are driven by heterogeneous figure mismatches rather than "
        "one stable error pattern."
    ),
}

VIS_TYPE_INVALID_SUMMARIES = {
    "Sankey": (
        "Sankey accumulates invalid traces mainly because many paper figures were "
        "classified as flow-like when they are actually workflow schematics, "
        "Minard-style flow maps, or topology graphs rather than true Sankey diagrams."
    ),
    "Chord Diagram": (
        "Chord Diagram is often confused with hierarchical edge bundling and other "
        "circular link views that look similar but encode a different structure."
    ),
    "Heatmap": (
        "Heatmap is frequently inflated by Self-Organizing Map views and dot/bubble "
        "matrices where value is encoded by marks instead of colored cells."
    ),
    "Choropleth": (
        "Choropleth invalids are dominated by geographic path and flow maps where "
        "locations are connected by lines instead of regions being shaded by value."
    ),
    "Dendrogram": (
        "Dendrogram invalids mostly come from cone trees and other hierarchical "
        "layouts that are structurally related but not standard 2D dendrograms."
    ),
    "Treemap": (
        "Treemap is often confused with nested Euler-style or containment diagrams "
        "that show hierarchy without treemap tiling."
    ),
    "Dot Plot": (
        "Dot Plot invalids are often UI components or annotation widgets rather "
        "than actual statistical dot plots."
    ),
}

VIS_TYPE_INVALID_LABELS = {
    "Sankey": "Flow-like figure confusion",
    "Chord Diagram": "Circular link view confusion",
    "Heatmap": "Dot / bubble matrix confusion",
    "Choropleth": "Flow map / route map confusion",
    "Dendrogram": "Hierarchical layout confusion",
    "Treemap": "Nested containment diagram",
    "Dot Plot": "UI / annotation panel",
}


def _truncate_text(text: str, limit: int = 180) -> str:
    text = " ".join((text or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _get_dimension_meta(dimension: str) -> dict:
    return DRIFT_DIMENSIONS[dimension]


def _get_dimension_theme_meta(dimension: str) -> dict[str, dict]:
    meta = _get_dimension_meta(dimension)
    theme_lookup = {theme["id"]: theme for theme in meta["themes"]}
    return {
        "other": {
            "id": "other",
            "label": meta["other_label"],
            "summary": (
                "The justification does not fit the main recurring patterns, or "
                "the wording is too specific to cluster confidently."
            ),
        },
        **theme_lookup,
    }


def _get_dimension_theme_order(dimension: str) -> list[str]:
    return [theme["id"] for theme in _get_dimension_meta(dimension)["themes"]] + ["other"]


def _score_drift_themes(note: str, dimension: str) -> list[dict]:
    text = " ".join((note or "").lower().split())
    if not text:
        return []

    theme_meta = _get_dimension_meta(dimension)
    matches = []
    for index, theme in enumerate(theme_meta["themes"]):
        score = 0
        for pattern in theme["patterns"]:
            if re.search(pattern, text):
                score += 1
        if score:
            matches.append(
                {
                    "id": theme["id"],
                    "label": theme["label"],
                    "score": score,
                    "order": index,
                }
            )

    matches.sort(key=lambda item: (-item["score"], item["order"]))
    return matches


def _build_drift_justification_analysis(
    *,
    dimension: str,
    vis_type: str | None = None,
    theme_id: str | None = None,
    severity: str | None = None,
    search: str = "",
    include_entries: bool = False,
) -> dict:
    dimension_meta = _get_dimension_meta(dimension)
    theme_meta = _get_dimension_theme_meta(dimension)
    theme_order = _get_dimension_theme_order(dimension)
    notes_field = dimension_meta["notes_field"]
    severity_field = dimension_meta["severity_field"]

    qs = (
        DriftAnnotation.objects.filter(trace__annotation_status="annotated")
        .exclude(**{notes_field: ""})
        .select_related("trace__figure__paper", "trace__artifact__source")
        .order_by("-annotated_at")
    )
    if vis_type:
        qs = qs.filter(trace__figure__vis_type=vis_type)
    if severity in DRIFT_SEVERITY_LABELS:
        qs = qs.filter(**{severity_field: severity})

    raw_entries = []
    theme_counts: Counter[str] = Counter()
    severity_counts: Counter[str] = Counter()
    theme_examples: dict[str, str] = {}
    vis_type_counts: Counter[str] = Counter()
    vis_type_theme_counts: dict[str, Counter[str]] = defaultdict(Counter)
    vis_type_severity_counts: dict[str, Counter[str]] = defaultdict(Counter)

    query = search.strip().lower()

    for annotation in qs:
        note = " ".join((getattr(annotation, notes_field) or "").split())
        if not note:
            continue

        matches = _score_drift_themes(note, dimension)
        matched_theme_ids = [match["id"] for match in matches]
        primary_theme_id = matched_theme_ids[0] if matched_theme_ids else "other"
        figure = annotation.trace.figure
        artifact_source = annotation.trace.artifact.source
        entry = {
            "id": annotation.id,
            "dimension": dimension,
            "dimension_label": dimension_meta["label"],
            "vis_type": figure.vis_type or "Unknown",
            "severity": getattr(annotation, severity_field),
            "severity_label": DRIFT_SEVERITY_LABELS.get(
                getattr(annotation, severity_field),
                getattr(annotation, severity_field).title(),
            ),
            "primary_theme_id": primary_theme_id,
            "primary_theme_label": theme_meta[primary_theme_id]["label"],
            "theme_ids": matched_theme_ids,
            "theme_labels": [theme_meta[item]["label"] for item in matched_theme_ids],
            "note": note,
            "note_short": _truncate_text(note, limit=220),
            "paper_title": figure.paper.title,
            "paper_year": figure.paper.year,
            "artifact_title": artifact_source.title,
            "artifact_url": artifact_source.url,
            "platform": artifact_source.platform,
        }

        if theme_id and primary_theme_id != theme_id:
            continue
        if query:
            haystack = " ".join(
                [
                    entry["vis_type"],
                    entry["note"],
                    entry["paper_title"],
                    entry["artifact_title"],
                    entry["platform"],
                    entry["primary_theme_label"],
                ]
            ).lower()
            if query not in haystack:
                continue

        raw_entries.append(entry)
        theme_counts[primary_theme_id] += 1
        severity_counts[entry["severity"]] += 1
        vis_type_counts[entry["vis_type"]] += 1
        vis_type_theme_counts[entry["vis_type"]][primary_theme_id] += 1
        vis_type_severity_counts[entry["vis_type"]][entry["severity"]] += 1
        theme_examples.setdefault(primary_theme_id, entry["note_short"])

    total = len(raw_entries)

    top_themes = [
        {
            "id": theme_id_key,
            "dimension": dimension,
            "dimension_label": dimension_meta["label"],
            "label": theme_meta[theme_id_key]["label"],
            "summary": theme_meta[theme_id_key]["summary"],
            "count": theme_counts[theme_id_key],
            "share": round((theme_counts[theme_id_key] / total) * 100) if total else 0,
            "example": theme_examples.get(theme_id_key, ""),
        }
        for theme_id_key in theme_order
        if theme_counts[theme_id_key]
    ]

    type_profiles = []
    for vis_type_key, count in vis_type_counts.items():
        dominant_theme_id, dominant_count = vis_type_theme_counts[vis_type_key].most_common(1)[0]
        type_profiles.append(
            {
                "dimension": dimension,
                "dimension_label": dimension_meta["label"],
                "vis_type": vis_type_key,
                "count": count,
                "dominant_theme_id": dominant_theme_id,
                "dominant_theme_label": theme_meta[dominant_theme_id]["label"],
                "dominant_theme_share": round((dominant_count / count) * 100) if count else 0,
                "major_share": round(
                    (vis_type_severity_counts[vis_type_key]["major"] / count) * 100
                ) if count else 0,
            }
        )
    type_profiles.sort(
        key=lambda row: (
            -row["count"],
            -row["major_share"],
            row["vis_type"],
        )
    )

    return {
        "dimension": dimension,
        "dimension_label": dimension_meta["label"],
        "total": total,
        "severity_counts": {
            key: severity_counts.get(key, 0) for key in ("major", "minor", "none")
        },
        "top_themes": top_themes,
        "type_profiles": type_profiles,
        "entries": raw_entries if include_entries else raw_entries[:12],
    }


def _build_pagination_context(request, entries: list, page_size: int = 20) -> tuple[list, dict]:
    paginator = Paginator(entries, page_size)
    page_number = request.GET.get("page") or 1
    page_obj = paginator.get_page(page_number)

    query = request.GET.copy()
    if "page" in query:
        query.pop("page")
    query_string = query.urlencode()

    return list(page_obj.object_list), {
        "page": page_obj.number,
        "pages": paginator.num_pages,
        "page_size": page_size,
        "total": paginator.count,
        "has_previous": page_obj.has_previous(),
        "has_next": page_obj.has_next(),
        "previous_page": page_obj.previous_page_number() if page_obj.has_previous() else None,
        "next_page": page_obj.next_page_number() if page_obj.has_next() else None,
        "start_index": page_obj.start_index() if paginator.count else 0,
        "end_index": page_obj.end_index() if paginator.count else 0,
        "query_string": query_string,
    }


def _build_filtered_drift_chart_context(analysis: dict) -> dict:
    entries = analysis["entries"]

    theme_severity = {
        row["id"]: {"label": row["label"], "major": 0, "minor": 0, "none": 0}
        for row in analysis["top_themes"][:8]
    }
    vis_type_counts: Counter[str] = Counter()
    vis_type_major_counts: Counter[str] = Counter()
    platform_counts: Counter[str] = Counter()
    year_counts: Counter[int] = Counter()

    for entry in entries:
        if entry["primary_theme_id"] in theme_severity:
            theme_severity[entry["primary_theme_id"]][entry["severity"]] += 1
        vis_type_counts[entry["vis_type"]] += 1
        if entry["severity"] == "major":
            vis_type_major_counts[entry["vis_type"]] += 1
        platform_counts[entry["platform"]] += 1
        if entry["paper_year"]:
            year_counts[int(entry["paper_year"])] += 1

    top_vis_types = []
    for vis_type, count in vis_type_counts.most_common(12):
        top_vis_types.append(
            {
                "vis_type": vis_type,
                "count": count,
                "major_share": round((vis_type_major_counts[vis_type] / count) * 100)
                if count else 0,
            }
        )

    return {
        "theme_severity_rows": [
            {
                "label": row["label"],
                "major": row["major"],
                "minor": row["minor"],
                "none": row["none"],
            }
            for row in theme_severity.values()
            if row["major"] or row["minor"] or row["none"]
        ],
        "vis_type_rows": top_vis_types,
        "platform_rows": [
            {"platform": platform, "count": count}
            for platform, count in platform_counts.most_common()
        ],
        "year_rows": [
            {"year": year, "count": year_counts[year]}
            for year in sorted(year_counts.keys())
        ],
    }


def _categorize_invalid_reason(reason: str) -> str:
    text = (reason or "").strip().lower()
    if not text:
        return "other_nonmatching"

    if re.search(
        r"conceptual|process diagram|workflow|pipeline|schematic|system diagram|"
        r"user interface|ui component|interface|toolbar|control panel|annotation mode|"
        r"decision-making loop|parameter space|performance space|design exploration|"
        r"nested diagram|euler",
        text,
    ):
        return "workflow_diagram"

    if re.search(
        r"flow map|geographic|cartographic|great-circle|troop movement|napoleon|"
        r"path diagram|world map|projection|route|map-based",
        text,
    ):
        return "map_like_flow"

    if re.search(
        r"mesh|triangulated|topolog|reeb|fiber surface|planar graph|polygon network|"
        r"vertices and edges|geometric graph|jacobi",
        text,
    ):
        return "topology_mesh"

    if re.search(
        r"hierarchical edge bund|cone tree|dot plot|bubble plot|u-matrix|"
        r"self-organizing map|ridgeline|themeriver|theme river|icicle|"
        r"force-directed|decision tree|radial tree|contour plot|box plot|"
        r"starburst|node-link",
        text,
    ):
        return "adjacent_chart"

    if re.search(
        r"scientific visualization|scalar field|vortex|particle collision|detector|"
        r"fluid flow|domain-specific",
        text,
    ):
        return "scientific_figure"

    if re.search(
        r"repository|notebook|code|unrelated|no code|complete disconnect|"
        r"contains no implementation|contains no code|generic educational",
        text,
    ):
        return "repository_mismatch"

    return "other_nonmatching"


def _summarize_invalid_reasons(vis_type: str, reasons: list[str]) -> dict:
    if not reasons:
        return {
            "dominant_label": INVALID_REASON_LABELS["other_nonmatching"],
            "dominant_count": 0,
            "dominant_share": 0,
            "summary": INVALID_REASON_SUMMARIES["other_nonmatching"],
            "example_reason": "",
        }

    category_counts: Counter[str] = Counter()
    category_examples: dict[str, str] = {}

    for reason in reasons:
        category = _categorize_invalid_reason(reason)
        category_counts[category] += 1
        category_examples.setdefault(category, reason)

    dominant_category, dominant_count = category_counts.most_common(1)[0]
    summary = VIS_TYPE_INVALID_SUMMARIES.get(
        vis_type,
        INVALID_REASON_SUMMARIES.get(dominant_category, INVALID_REASON_SUMMARIES["other_nonmatching"]),
    )

    return {
        "dominant_label": VIS_TYPE_INVALID_LABELS.get(
            vis_type,
            INVALID_REASON_LABELS.get(
                dominant_category,
                INVALID_REASON_LABELS["other_nonmatching"],
            ),
        ),
        "dominant_count": dominant_count,
        "dominant_share": round((dominant_count / len(reasons)) * 100),
        "summary": summary,
        "example_reason": _truncate_text(category_examples.get(dominant_category, "")),
    }


def dashboard(request):
    vis_figures = PaperFigure.objects.filter(is_visualization=True)
    annotated_traces = Trace.objects.filter(annotation_status="annotated")
    invalid_traces = list(
        Trace.objects.filter(annotation_status="invalid")
        .select_related("figure")
    )
    published_narratives = Narrative.objects.filter(status="published")
    drift_annotations = list(
        DriftAnnotation.objects.select_related("trace__figure")
    )

    context = {
        "total_papers": Paper.objects.count(),
        "total_figures": vis_figures.count(),
        "total_notebooks": RepoSource.objects.count(),
        "total_traces": annotated_traces.count(),
        "total_invalid_traces": len(invalid_traces),
        "total_narratives": published_narratives.count(),
        "total_drift_annotations": len(drift_annotations),
    }

    academic_rows = list(
        vis_figures
        .exclude(vis_type="")
        .values("vis_type")
        .annotate(count=Count("id"))
        .order_by("-count")
    )
    academic_map = {row["vis_type"]: row["count"] for row in academic_rows}
    context["academic_dist"] = json.dumps(academic_rows[:20])

    repo_types: Counter[str] = Counter()
    for artifact in RepoArtifact.objects.exclude(detected_chart_types__in=["[]", ""]):
        for t in artifact.get_detected_chart_types():
            if t:
                repo_types[t] += 1
    repo_rows = [{"vis_type": k, "count": v} for k, v in repo_types.most_common()]
    repo_map = dict(repo_types)
    context["repo_dist"] = json.dumps(repo_rows[:20])

    trace_rows = list(
        annotated_traces.exclude(figure__vis_type="")
        .values("figure__vis_type")
        .annotate(count=Count("id"))
        .order_by("-count")
    )
    trace_map = {row["figure__vis_type"]: row["count"] for row in trace_rows}
    invalid_map: Counter[str] = Counter()
    invalid_reasons_by_type: dict[str, list[str]] = defaultdict(list)
    for trace in invalid_traces:
        vis_type = trace.figure.vis_type or ""
        if not vis_type:
            continue
        invalid_map[vis_type] += 1
        if trace.invalid_reason:
            invalid_reasons_by_type[vis_type].append(trace.invalid_reason)

    yearly_papers = {
        row["year"]: row["count"]
        for row in (
            Paper.objects.exclude(year=None)
            .values("year")
            .annotate(count=Count("id"))
            .order_by("year")
        )
    }
    yearly_figures = {
        row["paper__year"]: row["count"]
        for row in (
            vis_figures.exclude(paper__year=None)
            .values("paper__year")
            .annotate(count=Count("id"))
            .order_by("paper__year")
        )
    }
    years = sorted(set(yearly_papers) | set(yearly_figures))
    context["yearly_series"] = json.dumps(
        [
            {
                "year": year,
                "papers": yearly_papers.get(year, 0),
                "figures": yearly_figures.get(year, 0),
            }
            for year in years
        ]
    )

    track_labels = dict(Paper.TRACK_CHOICES)
    platform_labels = dict(RepoSource.PLATFORM_CHOICES)
    track_rows = [
        {
            "track": row["track"],
            "label": track_labels.get(row["track"], row["track"]),
            "count": row["count"],
        }
        for row in (
            Paper.objects.values("track")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
    ]
    platform_rows = [
        {
            "platform": row["platform"],
            "label": platform_labels.get(row["platform"], row["platform"]),
            "count": row["count"],
        }
        for row in (
            RepoSource.objects.values("platform")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
    ]
    context["track_dist"] = json.dumps(track_rows)
    context["platform_dist"] = json.dumps(platform_rows)

    score_map = {"none": 0, "minor": 1, "major": 2}
    drift_profile = []
    for field, label in (
        ("encoding_drift", "Encoding"),
        ("interaction_drift", "Interaction"),
        ("task_drift", "Task"),
    ):
        counts = Counter(getattr(annotation, field) for annotation in drift_annotations)
        drift_profile.append(
            {
                "dimension": label,
                "none": counts.get("none", 0),
                "minor": counts.get("minor", 0),
                "major": counts.get("major", 0),
            }
        )
    context["drift_profile"] = json.dumps(drift_profile)

    drift_by_type: dict[str, dict[str, float]] = defaultdict(
        lambda: {"count": 0, "score": 0}
    )
    high_drift_annotations = 0
    for annotation in drift_annotations:
        vis_type = annotation.trace.figure.vis_type or "Unknown"
        severity = (
            score_map.get(annotation.encoding_drift, 0)
            + score_map.get(annotation.interaction_drift, 0)
            + score_map.get(annotation.task_drift, 0)
        )
        drift_by_type[vis_type]["count"] += 1
        drift_by_type[vis_type]["score"] += severity
        if (
            annotation.encoding_drift == "major"
            or annotation.interaction_drift == "major"
            or annotation.task_drift == "major"
        ):
            high_drift_annotations += 1

    bridge_landscape = []
    for vis_type in sorted(
        set(academic_map) | set(repo_map) | set(trace_map) | set(invalid_map),
        key=lambda key: (
            -trace_map.get(key, 0),
            -invalid_map.get(key, 0),
            -(academic_map.get(key, 0) + repo_map.get(key, 0)),
            key,
        ),
    ):
        if not vis_type:
            continue
        verified_count = trace_map.get(vis_type, 0)
        invalid_count = int(invalid_map.get(vis_type, 0))
        reviewed_count = verified_count + invalid_count
        drift_count = int(drift_by_type[vis_type]["count"])
        average_drift = (
            round(drift_by_type[vis_type]["score"] / drift_count, 2)
            if drift_count
            else None
        )
        invalid_summary = _summarize_invalid_reasons(
            vis_type,
            invalid_reasons_by_type.get(vis_type, []),
        )
        bridge_landscape.append(
            {
                "vis_type": vis_type,
                "academic_count": academic_map.get(vis_type, 0),
                "repo_count": repo_map.get(vis_type, 0),
                "trace_count": verified_count,
                "invalid_count": invalid_count,
                "reviewed_count": reviewed_count,
                "invalid_share": (
                    round((invalid_count / reviewed_count) * 100)
                    if reviewed_count
                    else 0
                ),
                "drift_count": drift_count,
                "average_drift": average_drift,
                "dominant_invalid_label": invalid_summary["dominant_label"],
                "dominant_invalid_share": invalid_summary["dominant_share"],
                "dominant_invalid_summary": invalid_summary["summary"],
                "dominant_invalid_example": invalid_summary["example_reason"],
            }
        )
    context["bridge_landscape"] = json.dumps(bridge_landscape)

    comparison_rows = sorted(
        [
            {
                "vis_type": vis_type,
                "academic_count": academic_map.get(vis_type, 0),
                "repo_count": repo_map.get(vis_type, 0),
                "trace_count": trace_map.get(vis_type, 0),
            }
            for vis_type in set(academic_map) | set(repo_map)
            if academic_map.get(vis_type, 0) or repo_map.get(vis_type, 0)
        ],
        key=lambda row: (
            -(row["academic_count"] + row["repo_count"]),
            -row["trace_count"],
            row["vis_type"],
        ),
    )[:16]
    context["comparison_dist"] = json.dumps(comparison_rows)

    shared_types = set(academic_map) & set(repo_map)
    context["shared_vis_types"] = len(shared_types)
    context["high_drift_annotations"] = high_drift_annotations
    reviewed_trace_total = context["total_traces"] + context["total_invalid_traces"]
    context["reviewed_trace_total"] = reviewed_trace_total
    context["invalid_trace_share"] = (
        round((context["total_invalid_traces"] / reviewed_trace_total) * 100)
        if reviewed_trace_total
        else 0
    )
    context["confirmed_trace_share"] = (
        round((context["total_traces"] / reviewed_trace_total) * 100)
        if reviewed_trace_total
        else 0
    )
    context["high_drift_share"] = (
        round((high_drift_annotations / len(drift_annotations)) * 100)
        if drift_annotations
        else 0
    )

    academic_leader = academic_rows[0] if academic_rows else {"vis_type": "None", "count": 0}
    repo_leader = repo_rows[0] if repo_rows else {"vis_type": "None", "count": 0}
    drift_leader = next(
        (
            row
            for row in sorted(
                bridge_landscape,
                key=lambda row: (
                    -(row["average_drift"] or -1),
                    -row["drift_count"],
                    -row["trace_count"],
                ),
            )
            if row["average_drift"] is not None and row["drift_count"] >= 5
        ),
        {"vis_type": "None", "average_drift": 0, "drift_count": 0},
    )
    context["headline_insights"] = [
        {
            "label": "Academic leader",
            "vis_type": academic_leader["vis_type"],
            "value": academic_leader["count"],
            "tone": "academic",
        },
        {
            "label": "Repository leader",
            "vis_type": repo_leader["vis_type"],
            "value": repo_leader["count"],
            "tone": "repo",
        },
        {
            "label": "Review yield",
            "vis_type": f'{context["confirmed_trace_share"]}% valid traces',
            "value": f'{context["total_traces"]} valid traces from {reviewed_trace_total} reviewed candidates',
            "tone": "bridge",
        },
        {
            "label": "Highest drift",
            "vis_type": drift_leader["vis_type"],
            "value": drift_leader["average_drift"],
            "tone": "warning",
        },
    ]

    context["bridge_spotlights"] = [
        row
        for row in sorted(
            bridge_landscape,
            key=lambda row: (-row["trace_count"], -(row["average_drift"] or 0)),
        )
        if row["trace_count"] > 0
    ][:6]
    context["invalid_reason_spotlights"] = [
        row
        for row in sorted(
            bridge_landscape,
            key=lambda row: (
                -row["invalid_count"],
                -row["invalid_share"],
                -row["trace_count"],
                row["vis_type"],
            ),
        )
        if row["invalid_count"] > 0
    ][:6]

    dimension_analyses = {
        dimension: _build_drift_justification_analysis(dimension=dimension)
        for dimension in DRIFT_DIMENSION_ORDER
    }
    context["drift_justification_total"] = sum(
        analysis["total"] for analysis in dimension_analyses.values()
    )
    context["drift_dimension_rows"] = json.dumps(
        [
            {
                "dimension": dimension,
                "label": analysis["dimension_label"],
                "total": analysis["total"],
                "major": analysis["severity_counts"]["major"],
                "minor": analysis["severity_counts"]["minor"],
                "none": analysis["severity_counts"]["none"],
            }
            for dimension, analysis in dimension_analyses.items()
        ]
    )
    context["drift_dimension_theme_rows"] = json.dumps(
        {
            dimension: [
                {
                    "label": row["label"],
                    "count": row["count"],
                    "share": row["share"],
                }
                for row in analysis["top_themes"][:5]
            ]
            for dimension, analysis in dimension_analyses.items()
        }
    )
    context["drift_dimension_spotlights"] = [
        {
            "dimension": dimension,
            "dimension_label": analysis["dimension_label"],
            "total": analysis["total"],
            "top_theme": analysis["top_themes"][0] if analysis["top_themes"] else None,
        }
        for dimension, analysis in dimension_analyses.items()
    ]
    context["drift_theme_spotlights"] = [
        row
        for dimension in DRIFT_DIMENSION_ORDER
        for row in dimension_analyses[dimension]["top_themes"][:2]
    ]

    return render(request, "ui/dashboard.html", context)


# ── Explore ────────────────────────────────────────────────────────────────────

def explore(request, vis_type):
    # Decode URL-encoded type name
    from urllib.parse import unquote
    vis_type = unquote(vis_type)

    figure_qs = (
        PaperFigure.objects.filter(vis_type=vis_type, is_visualization=True)
        .select_related("paper")
    )
    figures = figure_qs.order_by("-paper__year", "figure_index")[:18]

    # Repository artifacts that detected this type — DB-side filter on the JSON field
    # SQLite supports LIKE on text columns; detected_chart_types is a JSON array string
    artifact_qs = RepoArtifact.objects.select_related("source").filter(
        detected_chart_types__icontains=f'"{vis_type}"'
    )
    artifacts = list(artifact_qs.order_by("-source__stars")[:12])

    # Drift summary for this type
    trace_qs = Trace.objects.filter(
        figure__vis_type=vis_type,
        annotation_status="annotated",
    )
    invalid_trace_qs = Trace.objects.filter(
        figure__vis_type=vis_type,
        annotation_status="invalid",
    )
    drift_qs = DriftAnnotation.objects.filter(
        trace__figure__vis_type=vis_type,
        trace__annotation_status="annotated",
    )
    enc_counts = Counter(d.encoding_drift for d in drift_qs)
    inter_counts = Counter(d.interaction_drift for d in drift_qs)
    task_counts = Counter(d.task_drift for d in drift_qs)

    drift_data = json.dumps({
        "encoding": dict(enc_counts),
        "interaction": dict(inter_counts),
        "task": dict(task_counts),
    })

    year_rows = list(
        figure_qs.exclude(paper__year=None)
        .values("paper__year")
        .annotate(
            figure_count=Count("id"),
            paper_count=Count("paper", distinct=True),
        )
        .order_by("paper__year")
    )
    year_series = json.dumps(
        [
            {
                "year": row["paper__year"],
                "figures": row["figure_count"],
                "papers": row["paper_count"],
            }
            for row in year_rows
        ]
    )

    platform_labels = dict(RepoSource.PLATFORM_CHOICES)
    platform_rows = [
        {
            "platform": row["source__platform"],
            "label": platform_labels.get(row["source__platform"], row["source__platform"]),
            "count": row["count"],
        }
        for row in (
            artifact_qs.values("source__platform")
            .annotate(count=Count("id"))
            .order_by("-count")
        )
    ]

    libraries = Counter()
    for artifact in artifact_qs:
        libraries.update(artifact.get_detected_libraries())
    library_rows = [
        {"name": name, "count": count}
        for name, count in libraries.most_common(6)
    ]

    severity_map = {"none": 0, "minor": 1, "major": 2}
    drift_rows = list(drift_qs)
    avg_drift = (
        round(
            sum(
                severity_map.get(d.encoding_drift, 0)
                + severity_map.get(d.interaction_drift, 0)
                + severity_map.get(d.task_drift, 0)
                for d in drift_rows
            ) / len(drift_rows),
            2,
        )
        if drift_rows
        else None
    )
    dimension_total = len(drift_rows) * 3
    major_total = (
        sum(1 for d in drift_rows if d.encoding_drift == "major")
        + sum(1 for d in drift_rows if d.interaction_drift == "major")
        + sum(1 for d in drift_rows if d.task_drift == "major")
    )
    major_share_for_type = (
        round((major_total / dimension_total) * 100)
        if dimension_total
        else 0
    )
    invalid_reasons_for_type = [
        reason for reason in invalid_trace_qs.values_list("invalid_reason", flat=True)
        if reason
    ]
    invalid_summary_for_type = _summarize_invalid_reasons(vis_type, invalid_reasons_for_type)
    total_traces_for_type = trace_qs.count()
    total_invalid_traces_for_type = invalid_trace_qs.count()
    reviewed_trace_candidates_for_type = total_traces_for_type + total_invalid_traces_for_type
    confirmed_trace_share_for_type = (
        round((total_traces_for_type / reviewed_trace_candidates_for_type) * 100)
        if reviewed_trace_candidates_for_type
        else 0
    )
    if total_invalid_traces_for_type == 0:
        dominant_invalid_label_for_type = "No invalid reviews yet"
        dominant_invalid_share_for_type = 0
        dominant_invalid_summary_for_type = (
            "All reviewed trace candidates for this type currently remain in the "
            "confirmed-valid set."
        )
    else:
        dominant_invalid_label_for_type = invalid_summary_for_type["dominant_label"]
        dominant_invalid_share_for_type = invalid_summary_for_type["dominant_share"]
        dominant_invalid_summary_for_type = invalid_summary_for_type["summary"]

    dimension_analyses_for_type = {
        dimension: _build_drift_justification_analysis(
            dimension=dimension,
            vis_type=vis_type,
            include_entries=True,
        )
        for dimension in DRIFT_DIMENSION_ORDER
    }

    narrative = Narrative.objects.filter(vis_type=vis_type, status="published").first()

    from core.taxonomy import VIS_TYPES
    context = {
        "vis_type": vis_type,
        "figures": figures,
        "artifacts": artifacts,
        "drift_data": drift_data,
        "narrative": narrative,
        "all_vis_types": VIS_TYPES,
        "total_figures_for_type": figure_qs.count(),
        "total_papers_for_type": figure_qs.values("paper").distinct().count(),
        "total_artifacts_for_type": artifact_qs.count(),
        "total_traces_for_type": total_traces_for_type,
        "total_invalid_traces_for_type": total_invalid_traces_for_type,
        "reviewed_trace_candidates_for_type": reviewed_trace_candidates_for_type,
        "confirmed_trace_share_for_type": confirmed_trace_share_for_type,
        "year_series": year_series,
        "platform_rows": json.dumps(platform_rows),
        "library_rows": library_rows,
        "avg_drift_for_type": avg_drift,
        "major_share_for_type": major_share_for_type,
        "drift_annotations_for_type": len(drift_rows),
        "dominant_invalid_label_for_type": dominant_invalid_label_for_type,
        "dominant_invalid_share_for_type": dominant_invalid_share_for_type,
        "dominant_invalid_summary_for_type": dominant_invalid_summary_for_type,
        "drift_justification_total_for_type": sum(
            analysis["total"] for analysis in dimension_analyses_for_type.values()
        ),
        "drift_dimension_rows_for_type": json.dumps(
            [
                {
                    "dimension": dimension,
                    "label": analysis["dimension_label"],
                    "total": analysis["total"],
                    "major": analysis["severity_counts"]["major"],
                    "minor": analysis["severity_counts"]["minor"],
                    "none": analysis["severity_counts"]["none"],
                }
                for dimension, analysis in dimension_analyses_for_type.items()
            ]
        ),
        "drift_justification_panels_for_type": [
            {
                "dimension": dimension,
                "dimension_label": analysis["dimension_label"],
                "total": analysis["total"],
                "top_themes": analysis["top_themes"][:2],
                "entries": analysis["entries"][:2],
            }
            for dimension, analysis in dimension_analyses_for_type.items()
        ],
    }
    return render(request, "ui/explore.html", context)


def encoding_justifications(request):
    selected_dimension = request.GET.get("dimension", "encoding").strip()
    selected_vis_type = request.GET.get("vis_type", "").strip()
    selected_theme = request.GET.get("theme", "").strip()
    selected_severity = request.GET.get("severity", "").strip()
    search_query = request.GET.get("q", "").strip()

    if selected_dimension not in DRIFT_DIMENSIONS:
        selected_dimension = "encoding"

    theme_meta = _get_dimension_theme_meta(selected_dimension)

    if selected_theme not in theme_meta:
        selected_theme = ""
    if selected_severity not in DRIFT_SEVERITY_LABELS:
        selected_severity = ""

    analysis = _build_drift_justification_analysis(
        dimension=selected_dimension,
        vis_type=selected_vis_type or None,
        theme_id=selected_theme or None,
        severity=selected_severity or None,
        search=search_query,
        include_entries=True,
    )
    chart_context = _build_filtered_drift_chart_context(analysis)
    paginated_entries, pagination = _build_pagination_context(
        request,
        analysis["entries"],
        page_size=20,
    )
    dimension_summaries = {
        dimension: _build_drift_justification_analysis(
            dimension=dimension,
            vis_type=selected_vis_type or None,
        )
        for dimension in DRIFT_DIMENSION_ORDER
    }

    available_vis_types = list(
        PaperFigure.objects.filter(traces__annotation_status="annotated")
        .exclude(vis_type="")
        .values_list("vis_type", flat=True)
        .distinct()
        .order_by("vis_type")
    )

    context = {
        "selected_dimension": selected_dimension,
        "selected_vis_type": selected_vis_type,
        "selected_theme": selected_theme,
        "selected_severity": selected_severity,
        "search_query": search_query,
        "available_vis_types": available_vis_types,
        "dimension_choices": [
            {"id": dimension, "label": DRIFT_DIMENSIONS[dimension]["label"]}
            for dimension in DRIFT_DIMENSION_ORDER
        ],
        "theme_choices": [theme_meta[theme_id] for theme_id in _get_dimension_theme_order(selected_dimension)],
        "analysis": {
            **analysis,
            "entries": paginated_entries,
        },
        "dimension_summaries": [
            {
                "dimension": dimension,
                "dimension_label": summary["dimension_label"],
                "total": summary["total"],
                "top_theme": summary["top_themes"][0] if summary["top_themes"] else None,
                "top_themes": summary["top_themes"][:3],
                "severity_counts": summary["severity_counts"],
                "severity_shares": {
                    key: round((summary["severity_counts"][key] / summary["total"]) * 100, 1)
                    if summary["total"] else 0
                    for key in ("major", "minor", "none")
                },
            }
            for dimension, summary in dimension_summaries.items()
        ],
        "pagination": pagination,
        "analysis_theme_rows": json.dumps(
            [
                {
                    "label": row["label"],
                    "count": row["count"],
                    "share": row["share"],
                }
                for row in analysis["top_themes"][:6]
            ]
        ),
        "analysis_theme_severity_rows": json.dumps(chart_context["theme_severity_rows"]),
        "analysis_vis_type_rows": json.dumps(chart_context["vis_type_rows"]),
        "analysis_platform_rows": json.dumps(chart_context["platform_rows"]),
        "analysis_year_rows": json.dumps(chart_context["year_rows"]),
    }
    return render(request, "ui/encoding_justifications.html", context)


# ── Activity monitor ───────────────────────────────────────────────────────────

def activity(request):
    """Render the live pipeline monitor page."""
    from academic.models import PaperFigure, Paper
    from repository.models import RepoSource
    from tracing.models import Trace, Narrative

    stats = {
        "papers":            Paper.objects.count(),
        "figures":           PaperFigure.objects.filter(is_visualization=True).count(),
        "notebooks":         RepoSource.objects.count(),
        "traces":            Trace.objects.filter(verified=True).count(),
        "drift_annotations": DriftAnnotation.objects.count(),
        "narratives":        Narrative.objects.count(),
    }

    summary_items = [
        {"label": "Papers",            "key": "papers",            "value": stats["papers"]},
        {"label": "Figures (vis)",     "key": "figures",           "value": stats["figures"]},
        {"label": "Notebooks",         "key": "notebooks",         "value": stats["notebooks"]},
        {"label": "Traces",            "key": "traces",            "value": stats["traces"]},
        {"label": "Drift annotations", "key": "drift_annotations", "value": stats["drift_annotations"]},
        {"label": "Narratives",        "key": "narratives",        "value": stats["narratives"]},
    ]

    agent_rows = [
        {"cmd": "ingest_papers",       "arm": "academic"},
        {"cmd": "fetch_abstracts",     "arm": "academic"},
        {"cmd": "download_pdfs",       "arm": "academic"},
        {"cmd": "extract_figures",     "arm": "academic"},
        {"cmd": "classify_figures",    "arm": "academic"},
        {"cmd": "enrich_metadata",     "arm": "academic"},
        {"cmd": "crawl_repos",         "arm": "repo"},
        {"cmd": "detect_chart_types",  "arm": "repo"},
        {"cmd": "build_traces",        "arm": "bridge"},
        {"cmd": "annotate_drift",      "arm": "bridge"},
    ]
    return render(request, "ui/activity.html", {
        "stats": stats,
        "summary_items": summary_items,
        "agent_rows": agent_rows,
    })


def activity_stream(request):
    """
    SSE endpoint — streams JSON events to the browser.
    Each event is a JSON object from core.agent_log.emit().

    Supports Last-Event-ID for reconnect replay: the subscribe() call
    returns a queue pre-filled with recent events so a reconnecting client
    catches up immediately without waiting for the next emit().
    """
    q = subscribe()

    def event_generator():
        # Send a hello/retry directive so browser reconnects quickly (2s)
        yield "retry: 2000\n\n"
        try:
            while True:
                try:
                    payload = q.get(timeout=20)
                    # Parse to get event_id for the SSE id: field
                    try:
                        ev = json.loads(payload)
                        eid = ev.get("event_id", "")
                        yield f"id: {eid}\ndata: {payload}\n\n"
                    except Exception:
                        yield f"data: {payload}\n\n"
                except queue.Empty:
                    # Heartbeat to keep connection alive
                    yield ": heartbeat\n\n"
                except Exception:
                    yield ": heartbeat\n\n"
        finally:
            unsubscribe(q)

    response = StreamingHttpResponse(
        event_generator(),
        content_type="text/event-stream",
    )
    response["Cache-Control"] = "no-cache"
    response["X-Accel-Buffering"] = "no"
    return response


def activity_status(request):
    """
    Polling fallback endpoint — returns latest known agent status as JSON.
    The JS polls this every 3s so the UI recovers even if SSE events were missed.
    Used only as a safety net, not as the primary update mechanism.
    """
    return JsonResponse({"agents": get_agent_status()})


# ── Pipeline run endpoints ─────────────────────────────────────────────────────

# Allowed commands — whitelist for security
ALLOWED_COMMANDS = {
    "ingest_papers",
    "fetch_abstracts",
    "download_pdfs",
    "extract_figures",
    "classify_figures",
    "enrich_metadata",
    "crawl_repos",
    "detect_chart_types",
    "build_traces",
    "annotate_drift",
}

# Track running commands to prevent duplicate runs
_running: set[str] = set()
_running_lock = threading.Lock()


@csrf_exempt
@require_POST
def run_command(request, command):
    """
    Trigger a Django management command in a background thread.
    The command's output streams into the SSE event queue via core.agent_log.emit().
    """
    if command not in ALLOWED_COMMANDS:
        return JsonResponse({"error": f"Unknown command: {command}"}, status=400)

    with _running_lock:
        if command in _running:
            return JsonResponse({"error": f"{command} is already running"}, status=409)
        _running.add(command)

    # Schema-driven param parsing — no per-command if/elif blocks needed.
    # Each entry: param_name -> {type, choices?, default?}
    # "bool" params are truthy when POST value is non-empty and not "0"/"false".
    _PAPER_SOURCES   = ["all", "visimages", "seed_doi", "vis2019", "vis2020", "vis2021", "vis2022", "vis2023", "vis2024", "vis2025"]
    _DL_SOURCES      = ["all", "seed_doi", "vis2019", "vis2020", "vis2021", "vis2022", "vis2023", "vis2024", "vis2025"]
    COMMAND_PARAMS = {
        "ingest_papers":     {"source":         {"type": "choice",  "choices": _PAPER_SOURCES, "default": "all"}},
        "fetch_abstracts":   {"source":         {"type": "choice",  "choices": _PAPER_SOURCES, "default": "all"},
                              "limit":          {"type": "int"},
                              "dry_run":        {"type": "bool"},
                              "fetch_dois":     {"type": "bool"}},
        "download_pdfs":     {"source":         {"type": "choice",  "choices": _DL_SOURCES,    "default": "all"},
                              "reset":          {"type": "bool"}},
        "extract_figures":   {"limit":          {"type": "int"}},
        "classify_figures":  {"limit":          {"type": "int"},
                              "reset":          {"type": "bool"}},
        "enrich_metadata":   {"limit":          {"type": "int"}},
        "crawl_repos":       {"platform":       {"type": "choice",  "choices": ["all", "kaggle", "github", "observablehq"],   "default": "all"},
                              "keyword_source": {"type": "choice",  "choices": ["taxonomy", "db", "both"],                     "default": "both"}},
        "detect_chart_types":{"method":         {"type": "choice",  "choices": ["all", "a_only", "aplus_only", "b_only"],      "default": "all"}},
        "build_traces":      {"dry_run":        {"type": "bool"}},
        "annotate_drift":    {"limit":          {"type": "int"}},
    }

    command_kwargs: dict = {}
    for param, spec in COMMAND_PARAMS.get(command, {}).items():
        raw = request.POST.get(param, "").strip()
        if spec["type"] == "choice":
            val = raw if raw in spec.get("choices", []) else spec.get("default")
            if val is not None:
                command_kwargs[param] = val
        elif spec["type"] == "int":
            if raw:
                try:
                    command_kwargs[param] = int(raw)
                except (ValueError, TypeError):
                    pass
            elif "default" in spec:
                command_kwargs[param] = spec["default"]
        elif spec["type"] == "bool":
            if raw and raw not in ("0", "false"):
                command_kwargs[param] = True

    def _run():
        try:
            emit(agent=command, status="started", message=f"Triggered via UI: {command}")
            from django.core.management import call_command as django_call
            django_call(command, **command_kwargs)
        except Exception as exc:
            emit(agent=command, status="error",
                 message=f"Command {command} failed: {exc}", level="error")
        finally:
            # Sleep briefly so the SSE generator has time to dequeue and send
            # the final done/error event before this thread exits and the queue
            # goes idle. Without this, the last event can sit in the queue for
            # up to the heartbeat interval (20s) before the browser sees it.
            time.sleep(1.5)
            with _running_lock:
                _running.discard(command)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return JsonResponse({"status": "started", "command": command})
