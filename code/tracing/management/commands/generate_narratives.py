"""
Component 9 — Narrative Generation
Command: python manage.py generate_narratives [--limit N]

For each PaperFigure with >= 1 verified trace with drift annotation,
sends everything to the REASONING role (Gemini 3 Flash) to produce a
design anchor narrative (150-250 words).

One narrative per unique figure. Idempotent: skips figures already narrated.
"""

import json
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

from decouple import config
from django.core.management.base import BaseCommand

from academic.models import PaperFigure
from core.agent_log import emit
from core.config import get_role_concurrency, get_role_model
from core.llm_client import call_llm
from core.prompts.generate_narrative import (
    GENERATE_NARRATIVE_SYSTEM_PROMPT,
    generate_narrative_prompt,
)
from tracing.models import DriftAnnotation, Narrative, Trace

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Generate design anchor narratives for academic figures with drift evidence."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=None,
                            help="Generate at most N narratives")

    def handle(self, *args, **options):
        # Figures with >= 1 verified trace that has a drift annotation
        # and no existing narrative
        figures_with_traces = (
            PaperFigure.objects
            .filter(
                is_visualization=True,
                traces__verified=True,
                traces__drift_annotation__isnull=False,
            )
            .exclude(narratives__isnull=False)
            .distinct()
            .select_related("paper")
        )

        figures = list(figures_with_traces)
        if options["limit"]:
            figures = figures[: options["limit"]]

        total = len(figures)
        emit(agent="generate_narratives", status="started",
             message=f"Generating {total} narratives")

        generated = 0
        with ThreadPoolExecutor(max_workers=get_role_concurrency("REASONING")) as executor:
            future_to_fig = {
                executor.submit(self._generate_one, fig, i, total): fig
                for i, fig in enumerate(figures)
            }
            for future in as_completed(future_to_fig):
                fig = future_to_fig[future]
                try:
                    future.result()
                    generated += 1
                except Exception as exc:
                    logger.error("generate_narratives: figure %d failed: %s", fig.id, exc)
                    emit(agent="generate_narratives", status="error",
                         message=f"Figure {fig.id} failed: {exc}",
                         record_id=fig.id, level="error")

        emit(agent="generate_narratives", status="done",
             message=f"Done. {generated}/{total} narratives generated.")
        self.stdout.write(self.style.SUCCESS(f"Generated {generated}/{total} narratives."))

    def _generate_one(self, figure: PaperFigure, index: int, total: int) -> None:
        paper = figure.paper

        # Collect all verified traces with annotations for this figure
        traces = list(
            Trace.objects.filter(
                figure=figure,
                verified=True,
                drift_annotation__isnull=False,
            ).prefetch_related("drift_annotation")
        )

        if not traces:
            return

        drift_annotations = [t.drift_annotation for t in traces]
        trace_ids = [t.id for t in traces]

        # Aggregate drift counts
        encoding_counts = Counter(da.encoding_drift for da in drift_annotations)
        interaction_counts = Counter(da.interaction_drift for da in drift_annotations)
        task_counts = Counter(da.task_drift for da in drift_annotations)

        # Build drift notes sample (up to 3)
        notes_sample = [
            {
                "encoding_notes": da.encoding_notes,
                "interaction_notes": da.interaction_notes,
                "task_notes": da.task_notes,
            }
            for da in drift_annotations[:3]
        ]

        prompt = generate_narrative_prompt(
            paper_title=paper.title,
            paper_abstract=paper.abstract,
            paper_year=paper.year or 0,
            paper_track=paper.track,
            vis_type=figure.vis_type,
            total_traces=len(traces),
            encoding_drift_counts=dict(encoding_counts),
            interaction_drift_counts=dict(interaction_counts),
            task_drift_counts=dict(task_counts),
            drift_notes_sample=notes_sample,
        )

        narrative_text = call_llm(
            role="REASONING",
            prompt=prompt,
            system_prompt=GENERATE_NARRATIVE_SYSTEM_PROMPT,
        )

        model_used = get_role_model("REASONING")

        Narrative.objects.create(
            figure=figure,
            vis_type=figure.vis_type,
            narrative_text=str(narrative_text),
            source_traces=json.dumps(trace_ids),
            model_used=model_used,
        )

        emit(agent="generate_narratives", status="running",
             message=f"Figure {figure.id} ({figure.vis_type}): narrative generated",
             record_id=figure.id, progress=[index + 1, total])
