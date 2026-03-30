"""
Component 8 — Drift Annotation (multimodal revision)
Command: python manage.py annotate_drift [--limit N] [--reset-all]

For each verified Trace with annotation_status="unannotated":
  1. Resolve the figure image from disk.
  2. Call TRACE_ANNOTATE (Gemini 3.1 Flash Lite, multimodal) with both the
     figure image and the notebook code excerpt.
  3. Parse the "valid" field from the response:
     - valid=False  → set Trace.annotation_status="invalid", skip DriftAnnotation.
       The figure image was judged not to actually show the claimed vis_type.
     - valid=True   → create DriftAnnotation, set annotation_status="annotated".

Flags:
  --limit N      Process at most N traces (for incremental runs).
  --reset-all    Delete ALL existing DriftAnnotations and reset every
                 Trace.annotation_status to "unannotated". Use this to
                 discard old text-only Groq annotations before rerunning.

Idempotent: skips traces with annotation_status != "unannotated" (unless --reset-all).
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from core.agent_log import emit
from core.config import get_role_concurrency
from core.llm_client import call_llm
from core.prompts.annotate_drift import ANNOTATE_DRIFT_SYSTEM_PROMPT, annotate_drift_prompt
from tracing.models import DriftAnnotation, Trace

logger = logging.getLogger(__name__)

VALID_DRIFT = {"none", "minor", "major"}
CODE_EXCERPT_LINES = 200


class Command(BaseCommand):
    help = (
        "Annotate design drift for verified traces using Gemini multimodal — "
        "verifies the figure image then annotates drift. "
        "Use --reset-all to discard old annotations before rerunning."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Annotate at most N unannotated traces.",
        )
        parser.add_argument(
            "--reset-all",
            action="store_true",
            default=False,
            help=(
                "Delete ALL existing DriftAnnotations and reset every "
                "Trace.annotation_status to 'unannotated' before running. "
                "Use this to discard old text-only Groq annotations."
            ),
        )

    def handle(self, *args, **options):
        if options["reset_all"]:
            self._reset_all()

        # Only process verified traces that haven't been handled yet
        qs = (
            Trace.objects.filter(verified=True, annotation_status="unannotated")
            .select_related("figure__paper", "artifact__source")
        )

        traces = list(qs)
        if options["limit"]:
            traces = traces[: options["limit"]]

        total = len(traces)
        emit(
            agent="annotate_drift",
            status="started",
            message=f"Annotating drift (multimodal) for {total} unannotated traces",
        )

        annotated = 0
        invalidated = 0
        skipped_no_image = 0

        concurrency = get_role_concurrency("TRACE_ANNOTATE")
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_trace = {
                executor.submit(self._annotate_one, trace, i, total): trace
                for i, trace in enumerate(traces)
            }
            for future in as_completed(future_to_trace):
                trace = future_to_trace[future]
                try:
                    outcome = future.result()
                    if outcome == "annotated":
                        annotated += 1
                    elif outcome == "invalid":
                        invalidated += 1
                    else:
                        skipped_no_image += 1
                except Exception as exc:
                    logger.error("annotate_drift: trace %d failed: %s", trace.id, exc)
                    emit(
                        agent="annotate_drift",
                        status="error",
                        message=f"Trace {trace.id} failed: {exc}",
                        record_id=trace.id,
                        level="error",
                    )

        emit(
            agent="annotate_drift",
            status="done",
            message=(
                f"Done. {annotated} annotated, {invalidated} invalid "
                f"(figure type unconfirmed), {skipped_no_image} skipped (no image on disk)"
            ),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Annotated {annotated}, invalidated {invalidated}, "
                f"skipped {skipped_no_image} (no image) out of {total} traces."
            )
        )

    def _reset_all(self) -> None:
        """Delete all DriftAnnotations and reset all annotation_status to unannotated."""
        deleted, _ = DriftAnnotation.objects.all().delete()
        updated = Trace.objects.exclude(annotation_status="unannotated").update(
            annotation_status="unannotated"
        )
        emit(
            agent="annotate_drift",
            status="running",
            message=(
                f"--reset-all: deleted {deleted} DriftAnnotation rows, "
                f"reset {updated} Trace.annotation_status values."
            ),
        )
        self.stdout.write(
            self.style.WARNING(
                f"reset-all: {deleted} annotations deleted, {updated} traces reset."
            )
        )

    def _annotate_one(self, trace: Trace, index: int, total: int) -> str:
        """
        Annotate a single trace.
        Returns: "annotated" | "invalid" | "no_image"
        """
        figure = trace.figure
        paper = figure.paper
        artifact = trace.artifact

        # ── Resolve figure image path ─────────────────────────────────────────
        image_path = _resolve_image_path(figure.image_local_path)
        if image_path is None:
            logger.warning(
                "annotate_drift: figure %d has no image on disk (%s) — skipping trace %d",
                figure.id, figure.image_local_path, trace.id,
            )
            emit(
                agent="annotate_drift",
                status="running",
                message=(
                    f"Trace {trace.id}: skipped — figure image not found on disk "
                    f"({figure.image_local_path})"
                ),
                record_id=trace.id,
                progress=[index + 1, total],
            )
            return "no_image"

        # ── Build code excerpt ────────────────────────────────────────────────
        code_excerpt = _extract_code_excerpt(artifact)

        # ── Build prompt ──────────────────────────────────────────────────────
        prompt = annotate_drift_prompt(
            vis_type=figure.vis_type,
            paper_title=paper.title,
            paper_abstract=paper.abstract,
            paper_year=paper.year or 0,
            paper_track=paper.track,
            paper_keywords=paper.get_keywords(),
            detected_libraries=artifact.get_detected_libraries(),
            detected_chart_types=artifact.get_detected_chart_types(),
            notebook_code_excerpt=code_excerpt,
            platform=artifact.source.platform,
        )

        # ── Call TRACE_ANNOTATE (Gemini multimodal) ───────────────────────────
        result = call_llm(
            role="TRACE_ANNOTATE",
            prompt=prompt,
            image_path=str(image_path),
            response_format="json",
            system_prompt=ANNOTATE_DRIFT_SYSTEM_PROMPT,
        )

        if not isinstance(result, dict):
            raise ValueError(f"Unexpected response type: {type(result)}")

        # ── Parse validity ────────────────────────────────────────────────────
        is_valid = bool(result.get("valid", True))

        if not is_valid:
            reason = result.get("invalid_reason", "")
            trace.annotation_status = "invalid"
            trace.invalid_reason = reason
            trace.save(update_fields=["annotation_status", "invalid_reason"])
            emit(
                agent="annotate_drift",
                status="running",
                message=(
                    f"Trace {trace.id} [{figure.vis_type}]: INVALID — {reason or 'figure type unconfirmed'}"
                ),
                record_id=trace.id,
                progress=[index + 1, total],
            )
            return "invalid"

        # ── Valid — create DriftAnnotation ────────────────────────────────────
        encoding    = _normalise_drift(result.get("encoding", "none"))
        interaction = _normalise_drift(result.get("interaction", "none"))
        task        = _normalise_drift(result.get("task", "none"))

        DriftAnnotation.objects.create(
            trace=trace,
            encoding_drift=encoding,
            interaction_drift=interaction,
            task_drift=task,
            encoding_notes=result.get("encoding_notes", ""),
            interaction_notes=result.get("interaction_notes", ""),
            task_notes=result.get("task_notes", ""),
            annotated_by="llm",
        )

        trace.annotation_status = "annotated"
        trace.save(update_fields=["annotation_status"])

        emit(
            agent="annotate_drift",
            status="running",
            message=(
                f"Trace {trace.id} [{figure.vis_type}]: "
                f"enc={encoding}, inter={interaction}, task={task}"
            ),
            record_id=trace.id,
            progress=[index + 1, total],
        )
        return "annotated"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_image_path(image_local_path: str) -> Path | None:
    """
    Return the absolute Path to a figure image, or None if not found.
    image_local_path may be absolute or relative to MEDIA_ROOT.
    """
    if not image_local_path:
        return None
    p = Path(image_local_path)
    if p.is_absolute():
        return p if p.exists() else None
    abs_path = Path(settings.MEDIA_ROOT) / image_local_path
    return abs_path if abs_path.exists() else None


def _extract_code_excerpt(artifact) -> str:
    """Extract the first CODE_EXCERPT_LINES lines of code from an artifact.

    raw_content_path is stored relative to MEDIA_ROOT (e.g.
    'repos/notebooks/kaggle/user_nb/nb.ipynb'). Mirrors the same
    resolution logic used by _resolve_image_path.
    """
    if not artifact.raw_content_path:
        return ""
    raw_path = Path(artifact.raw_content_path)
    if not raw_path.is_absolute():
        raw_path = Path(settings.MEDIA_ROOT) / raw_path
    if not raw_path.exists():
        return ""
    try:
        raw = raw_path.read_text(encoding="utf-8", errors="replace")
        if artifact.raw_content_path.endswith(".ipynb"):
            try:
                nb = json.loads(raw)
                lines = []
                for cell in nb.get("cells", []):
                    if cell.get("cell_type") == "code":
                        src = cell.get("source", [])
                        lines.extend(src if isinstance(src, list) else [str(src)])
                raw = "".join(lines)
            except Exception:
                pass
        return "\n".join(raw.splitlines()[:CODE_EXCERPT_LINES])
    except Exception:
        return ""


def _normalise_drift(value: str) -> str:
    """Ensure drift value is one of none/minor/major."""
    v = str(value).lower().strip()
    if v in VALID_DRIFT:
        return v
    if any(w in v for w in ("major", "significant", "large", "high", "substantial")):
        return "major"
    if any(w in v for w in ("minor", "slight", "small", "low", "minimal")):
        return "minor"
    return "none"