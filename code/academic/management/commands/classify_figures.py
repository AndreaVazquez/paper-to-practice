"""
Component 3 — Figure Classification
Command: python manage.py classify_figures [--limit N] [--reset]

Two-step pipeline for each PaperFigure where is_visualization IS NULL:
  Step A — Relevance filter: is this a data visualization?
  Step B — Type classification: which VisImages taxonomy type?

Uses IMAGE role (Llama-4 Maverick on Groq), falls back to IMAGE_FALLBACK
(Llama-4 Scout) on 429. Processes in parallel via ThreadPoolExecutor.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from academic.models import PaperFigure
from core.agent_log import emit
from core.config import get_role_concurrency
from core.llm_client import call_llm
from core.prompts.classify_figure import (
    RELEVANCE_PROMPT,
    RELEVANCE_SYSTEM_PROMPT,
    TYPE_CLASSIFICATION_SYSTEM_PROMPT,
    type_classification_prompt,
)
from core.taxonomy import VIS_TYPES

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Classify paper figures as visualizations and assign vis_type."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=None,
                            help="Process at most N figures")
        parser.add_argument("--reset", action="store_true",
                            help="Reprocess figures that were already classified")

    def handle(self, *args, **options):
        qs = PaperFigure.objects.select_related("paper")
        if not options["reset"]:
            qs = qs.filter(is_visualization__isnull=True)
        else:
            # --reset redoes LLM classifications only — never overwrites human-annotated
            # ground truth from VisImages (annotation_source='visimages_json')
            qs = qs.exclude(annotation_source="visimages_json")

        figures = list(qs)
        if options["limit"]:
            figures = figures[: options["limit"]]

        total = len(figures)
        emit(agent="classify_figures", status="started",
             message=f"Classifying {total} figures (concurrency={get_role_concurrency('IMAGE')})")

        classified = 0
        is_vis_count = 0

        with ThreadPoolExecutor(max_workers=get_role_concurrency("IMAGE")) as executor:
            future_to_fig = {
                executor.submit(self._classify_one, fig, i, total): fig
                for i, fig in enumerate(figures)
            }
            for future in as_completed(future_to_fig):
                fig = future_to_fig[future]
                try:
                    is_vis = future.result()
                    classified += 1
                    if is_vis:
                        is_vis_count += 1
                except Exception as exc:
                    logger.error("classify_figures: figure %d failed: %s", fig.id, exc)
                    emit(agent="classify_figures", status="error",
                         message=f"Figure {fig.id} failed: {exc}",
                         record_id=fig.id, level="error")
                    # Mark as non-visualization so it doesn't re-enter the pool
                    # on the next run. annotation_source="" signals it was an
                    # error skip, not a real classification.
                    try:
                        fig.is_visualization = False
                        fig.annotation_source = "llm_classified"
                        fig.vis_type_raw = f"error: {exc}"
                        fig.save(update_fields=[
                            "is_visualization", "annotation_source", "vis_type_raw"
                        ])
                    except Exception:
                        pass  # DB save failure is non-fatal here

        emit(agent="classify_figures", status="done",
             message=f"Done. {classified} classified, {is_vis_count} are visualizations.")
        self.stdout.write(
            self.style.SUCCESS(
                f"Classified {classified} figures. {is_vis_count} are visualizations."
            )
        )

    def _classify_one(self, fig: PaperFigure, index: int, total: int) -> bool:
        """
        Classify a single figure. Returns True if it is a visualization.
        Updates the DB record in place.
        """
        # Resolve path — stored relative to MEDIA_ROOT, not CWD.
        _raw = fig.image_local_path
        if not _raw:
            fig.is_visualization = False
            fig.annotation_source = "llm_classified"
            fig.save(update_fields=["is_visualization", "annotation_source"])
            emit(agent="classify_figures", status="skipped",
                 message=f"Figure {fig.id}: no image path stored",
                 record_id=fig.id, level="warning")
            return False
        _p = Path(_raw) if Path(_raw).is_absolute() else Path(settings.MEDIA_ROOT) / _raw
        img_path = str(_p)
        if not _p.exists():
            fig.is_visualization = False
            fig.annotation_source = "llm_classified"
            fig.save(update_fields=["is_visualization", "annotation_source"])
            emit(agent="classify_figures", status="skipped",
                 message=f"Figure {fig.id}: image file not found at {img_path}",
                 record_id=fig.id, level="warning")
            return False

        # ── Step A: Relevance filter ───────────────────────────────────────────
        relevance_response = call_llm(
            role="IMAGE",
            prompt=RELEVANCE_PROMPT,
            image_path=img_path,
            system_prompt=RELEVANCE_SYSTEM_PROMPT,
        )

        is_vis, confidence_a = _parse_relevance_response(str(relevance_response))

        fig.is_visualization = is_vis
        fig.annotation_source = "llm_classified"

        if not is_vis:
            fig.save(update_fields=["is_visualization", "annotation_source"])
            emit(agent="classify_figures", status="running",
                 message=f"Figure {fig.id}: NOT a visualization (conf={confidence_a:.2f})",
                 record_id=fig.id, progress=[index + 1, total])
            return False

        # ── Step B: Type classification ────────────────────────────────────────
        type_prompt = type_classification_prompt(VIS_TYPES)
        type_response = call_llm(
            role="IMAGE",
            prompt=type_prompt,
            image_path=img_path,
            response_format="json",
            system_prompt=TYPE_CLASSIFICATION_SYSTEM_PROMPT,
        )

        vis_type = ""
        vis_confidence = None
        raw_json = ""

        if isinstance(type_response, dict):
            vis_type = type_response.get("type", "")
            vis_confidence = type_response.get("confidence")
            raw_json = json.dumps(type_response)
        elif isinstance(type_response, str):
            raw_json = type_response
            try:
                parsed = json.loads(type_response)
                vis_type = parsed.get("type", "")
                vis_confidence = parsed.get("confidence")
            except json.JSONDecodeError:
                pass

        # Validate vis_type against taxonomy
        if vis_type not in VIS_TYPES:
            vis_type = _find_closest_type(vis_type)

        fig.vis_type = vis_type
        fig.vis_type_confidence = vis_confidence
        fig.vis_type_raw = raw_json
        fig.save(update_fields=[
            "is_visualization", "vis_type", "vis_type_confidence",
            "vis_type_raw", "annotation_source",
        ])

        emit(agent="classify_figures", status="running",
             message=f"Figure {fig.id}: {vis_type} (conf={vis_confidence or 0:.2f})",
             record_id=fig.id, progress=[index + 1, total])

        return True


# ── Parsing helpers ────────────────────────────────────────────────────────────

def _parse_relevance_response(text: str) -> tuple[bool, float]:
    """
    Parse the YES/NO + confidence response from the relevance prompt.
    Returns (is_visualization, confidence).
    """
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    if not lines:
        return False, 0.0

    is_vis = lines[0].upper().startswith("YES")
    confidence = 0.0
    if len(lines) > 1:
        try:
            confidence = float(lines[1])
        except ValueError:
            # Try to extract a float from the line
            import re
            match = re.search(r"(\d+\.?\d*)", lines[1])
            if match:
                confidence = float(match.group(1))
                if confidence > 1.0:
                    confidence = confidence / 100.0

    return is_vis, confidence


def _find_closest_type(raw_type: str) -> str:
    """
    Try to fuzzy-match a model-returned type string against the taxonomy.
    Falls back to 'Other' if nothing matches.
    """
    if not raw_type:
        return "Other"

    raw_lower = raw_type.lower()
    for vt in VIS_TYPES:
        if vt.lower() == raw_lower:
            return vt
    # Partial match
    for vt in VIS_TYPES:
        if vt.lower() in raw_lower or raw_lower in vt.lower():
            return vt
    return "Other"