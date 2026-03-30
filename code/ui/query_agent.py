"""
Query Agent — translates a natural language query into a structured DB filter,
then executes it against the Django ORM.

Called by ui/views.py QueryView.
"""

import logging
from typing import Any

from core.llm_client import call_llm
from core.prompts.query_agent import QUERY_AGENT_SYSTEM_PROMPT, query_agent_prompt

logger = logging.getLogger(__name__)


def parse_query(user_query: str) -> dict[str, Any]:
    """
    Send the user query to the QUERY role LLM and return a structured filter dict.
    On any error, returns a safe default (all fields null).
    """
    try:
        result = call_llm(
            role="QUERY",
            prompt=query_agent_prompt(user_query),
            response_format="json",
            system_prompt=QUERY_AGENT_SYSTEM_PROMPT,
        )
        if isinstance(result, dict):
            return result
    except Exception as exc:
        logger.warning("query_agent: LLM call failed: %s", exc)

    return {
        "vis_type": None,
        "vis_type_category": None,
        "drift_type": None,
        "drift_severity": None,
        "year_from": None,
        "year_to": None,
        "track": None,
        "sort_by": None,
        "intent": "explore",
        "keywords": [],
    }


def execute_query(filters: dict[str, Any]) -> dict[str, Any]:
    """
    Execute a structured filter dict against the Django ORM.
    Returns a dict with keys: figures, artifacts, traces, narratives.
    """
    from academic.models import PaperFigure
    from tracing.models import Narrative, Trace

    # ── Paper figures ──────────────────────────────────────────────────────────
    fig_qs = PaperFigure.objects.filter(
        is_visualization=True
    ).select_related("paper")

    if filters.get("vis_type"):
        fig_qs = fig_qs.filter(vis_type=filters["vis_type"])
    elif filters.get("vis_type_category"):
        # Filter by category using taxonomy
        from core.taxonomy import TAXONOMY
        category = filters["vis_type_category"]
        subtypes = TAXONOMY.get(category, [])
        if subtypes:
            fig_qs = fig_qs.filter(vis_type__in=subtypes)

    if filters.get("year_from"):
        fig_qs = fig_qs.filter(paper__year__gte=filters["year_from"])
    if filters.get("year_to"):
        fig_qs = fig_qs.filter(paper__year__lte=filters["year_to"])
    if filters.get("track"):
        fig_qs = fig_qs.filter(paper__track=filters["track"])

    # Keyword search in paper title/abstract
    keywords = filters.get("keywords", [])
    for kw in keywords[:3]:
        from django.db.models import Q
        fig_qs = fig_qs.filter(
            Q(paper__title__icontains=kw) | Q(paper__abstract__icontains=kw)
        )

    figures = list(fig_qs[:50])

    # ── Traces ────────────────────────────────────────────────────────────────
    trace_qs = Trace.objects.filter(
        verified=True,
        figure__in=figures,
    ).select_related("figure", "artifact__source")

    if filters.get("drift_type") and filters.get("drift_severity"):
        drift_field = f"{filters['drift_type']}_drift"
        trace_qs = trace_qs.filter(
            drift_annotation__isnull=False,
            **{f"drift_annotation__{drift_field}": filters["drift_severity"]},
        )

    traces = list(trace_qs[:100])

    # ── Artifacts linked to matching traces ───────────────────────────────────
    artifact_ids = list({t.artifact_id for t in traces})
    from repository.models import RepoArtifact
    artifacts = list(RepoArtifact.objects.filter(id__in=artifact_ids).select_related("source"))

    # ── Narratives ────────────────────────────────────────────────────────────
    fig_ids = [f.id for f in figures]
    narratives = list(Narrative.objects.filter(figure_id__in=fig_ids).select_related("figure")[:20])

    return {
        "figures": figures,
        "artifacts": artifacts,
        "traces": traces,
        "narratives": narratives,
        "filters": filters,
    }
