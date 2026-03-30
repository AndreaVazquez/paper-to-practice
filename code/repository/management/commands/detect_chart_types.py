"""
Component 6 — Repository Chart Type Detection
Command: python manage.py detect_chart_types [--limit N] [--method METHOD]

Three-stage detection per artifact:

  Stage A  — Pattern matching (no LLM, instant):
               Scans code cells for STRONG_PATTERNS (specific function calls).
               If found → done, record method=code_analysis.

  Stage A+ — LLM code analysis (DETECT_CHARTS role, kimi-k2):
               Triggered when Stage A finds nothing.
               Sends code excerpt to model for intelligent analysis.
               If found → done, record method=llm_code_analysis.

  Stage B  — Image classification (IMAGE role):
               Last resort. Triggered only when both A and A+ find nothing
               AND the notebook has embedded output images.
               record method=image_classification.

After Stage A, WEAK_PATTERNS are applied only if all three stages found nothing,
as a final fallback (recorded as method=weak_pattern).

Idempotent: skips artifacts that already have detected_chart_types.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from django.core.management.base import BaseCommand
from django.utils import timezone

from core.agent_log import emit
from core.config import get_role_concurrency
from core.llm_client import call_llm
from core.prompts.detect_chart_types import (
    DETECT_CHARTS_SYSTEM_PROMPT,
    LIBRARY_IMPORT_PATTERNS,
    RELEVANCE_PROMPT,
    RELEVANCE_SYSTEM_PROMPT,
    STRONG_PATTERNS,
    TYPE_CLASSIFICATION_SYSTEM_PROMPT,
    WEAK_PATTERNS,
    detect_charts_prompt,
    type_classification_prompt,
)
from core.taxonomy import VIS_TYPES
from repository.models import RepoArtifact

logger = logging.getLogger(__name__)

# Cap on code lines sent to Method A+ to stay within context limits
_CODE_EXCERPT_LINES = 150
# Cap on output images tried in Method B
_MAX_OUTPUT_IMAGES = 5


class Command(BaseCommand):
    help = "Detect chart types in repository notebooks/scripts (3-stage pipeline)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit", type=int, default=None,
            help="Process at most N artifacts",
        )
        parser.add_argument(
            "--method",
            choices=["all", "a_only", "aplus_only", "b_only"],
            default="all",
            help=(
                "all        — run all stages in order (default)\n"
                "a_only     — pattern matching only (fast, no LLM)\n"
                "aplus_only — skip Stage A, run A+ on everything\n"
                "b_only     — image classification only\n"
            ),
        )

    def handle(self, *args, **options):
        qs = (
            RepoArtifact.objects
            .filter(detected_chart_types__in=["[]", "", None])
            .select_related("source")
        )
        artifacts = list(qs)
        if options["limit"]:
            artifacts = artifacts[: options["limit"]]

        total = len(artifacts)
        method_flag = options["method"]

        emit(
            agent="detect_chart_types", status="started",
            message=f"Detecting chart types in {total} artifacts (method={method_flag})",
        )

        processed = 0
        concurrency = get_role_concurrency("DETECT_CHARTS")

        with ThreadPoolExecutor(max_workers=max(concurrency, 1)) as executor:
            future_map = {
                executor.submit(
                    self._process_artifact, artifact, method_flag, i, total
                ): artifact
                for i, artifact in enumerate(artifacts)
            }
            for future in as_completed(future_map):
                artifact = future_map[future]
                try:
                    future.result()
                    processed += 1
                except Exception as exc:
                    logger.error(
                        "detect_chart_types: artifact %d failed: %s", artifact.id, exc,
                    )
                    emit(
                        agent="detect_chart_types", status="error",
                        message=f"Artifact {artifact.id} failed: {exc}",
                        record_id=artifact.id, level="error",
                    )

        emit(
            agent="detect_chart_types", status="done",
            message=f"Done. {processed}/{total} artifacts processed.",
        )
        self.stdout.write(self.style.SUCCESS(f"Processed {processed}/{total} artifacts."))

    # ── Per-artifact orchestration ─────────────────────────────────────────────

    def _process_artifact(
        self,
        artifact: RepoArtifact,
        method_flag: str,
        index: int,
        total: int,
    ) -> None:
        from django.conf import settings
        _raw = artifact.raw_content_path
        content_path = Path(_raw) if Path(_raw).is_absolute() else Path(settings.MEDIA_ROOT) / _raw
        if not content_path.exists():
            emit(
                agent="detect_chart_types", status="skipped",
                message=f"Artifact {artifact.id}: file not found at {content_path}",
                level="warning",
            )
            return

        try:
            raw = content_path.read_text(encoding="utf-8", errors="replace")
        except Exception as exc:
            logger.warning("Cannot read artifact %d: %s", artifact.id, exc)
            return

        if content_path.suffix == ".ipynb":
            code_cells, output_images = _parse_notebook(raw, content_path)
        else:
            code_cells = [raw]
            output_images = []

        all_code = "\n".join(code_cells)
        code_excerpt = "\n".join(all_code.splitlines()[:_CODE_EXCERPT_LINES])

        libraries: list[str] = _detect_libraries(all_code)
        chart_types: list[str] = []
        method_used = ""

        # ── Stage A — strong pattern matching ─────────────────────────────────
        if method_flag in ("all", "a_only"):
            chart_types = _run_strong_patterns(all_code)
            if chart_types:
                method_used = "code_analysis"

        # ── Stage A+ — LLM code analysis ──────────────────────────────────────
        if not chart_types and method_flag in ("all", "aplus_only"):
            aplus_types = _run_llm_code_analysis(code_excerpt)
            if aplus_types:
                chart_types = aplus_types
                method_used = "llm_code_analysis"

        # ── Stage B — output image classification ─────────────────────────────
        if not chart_types and output_images and method_flag in ("all", "b_only"):
            b_types = _classify_output_images(output_images)
            if b_types:
                chart_types = b_types
                method_used = "image_classification"

        # ── Weak patterns — last resort fallback ──────────────────────────────
        if not chart_types and method_flag in ("all", "a_only"):
            weak = _run_weak_patterns(all_code)
            if weak:
                chart_types = weak
                method_used = "weak_pattern"

        artifact.set_detected_libraries(libraries)
        artifact.set_detected_chart_types(chart_types)
        artifact.detection_method = method_used or "code_analysis"
        artifact.processed_at = timezone.now()
        artifact.save(update_fields=[
            "detected_libraries", "detected_chart_types",
            "detection_method", "processed_at",
        ])

        emit(
            agent="detect_chart_types", status="running",
            message=(
                f"[{artifact.source.platform}] Artifact {artifact.id}: "
                f"libs={libraries[:3]}, types={chart_types[:3]}, "
                f"method={method_used or 'none'}"
            ),
            record_id=artifact.id,
            progress=[index + 1, total],
        )


# ── Stage A: pattern matching ──────────────────────────────────────────────────

def _detect_libraries(code: str) -> list[str]:
    found = set()
    for lib_name, pattern in LIBRARY_IMPORT_PATTERNS:
        if pattern in code:
            found.add(lib_name)
    return sorted(found)


def _run_strong_patterns(code: str) -> list[str]:
    """Scan code for STRONG_PATTERNS. Returns deduplicated vis_type list."""
    found = set()
    for _lib, pattern, vis_type in STRONG_PATTERNS:
        if pattern in code:
            found.add(vis_type)
    return sorted(found)


def _run_weak_patterns(code: str) -> list[str]:
    """
    Apply WEAK_PATTERNS with additional guards.
    - plt.imshow( is only accepted as Heatmap if plt.colorbar( is also present
      (i.e. it's being used as a data display, not a photo viewer).
    - plt.plot( / ax.plot( accepted if no other strong chart signal was found.
    """
    found = set()
    for lib, pattern, vis_type in WEAK_PATTERNS:
        if pattern not in code:
            continue
        if pattern == "plt.imshow(":
            # Guard: require colorbar to distinguish data viz from photo display
            if "plt.colorbar(" not in code and "colorbar()" not in code:
                continue
        found.add(vis_type)
    return sorted(found)


# ── Stage A+: LLM code analysis ───────────────────────────────────────────────

def _run_llm_code_analysis(code_excerpt: str) -> list[str]:
    """
    Send code to DETECT_CHARTS role (kimi-k2) for intelligent chart detection.
    Returns a list of vis_type strings, or empty list on failure.
    """
    if not code_excerpt.strip():
        return []
    try:
        prompt = detect_charts_prompt(code_excerpt, VIS_TYPES)
        result = call_llm(
            role="DETECT_CHARTS",
            prompt=prompt,
            response_format="json",
            system_prompt=DETECT_CHARTS_SYSTEM_PROMPT,
        )
        if not isinstance(result, dict):
            return []
        raw_types = result.get("chart_types", [])
        if not isinstance(raw_types, list):
            return []
        # Validate against taxonomy
        valid = [t for t in raw_types if t in VIS_TYPES]
        confidence = result.get("confidence", "")
        # Discard low-confidence results — Stage B or weak patterns may do better
        if confidence == "low":
            return []
        return sorted(set(valid))
    except Exception as exc:
        logger.warning("Method A+ LLM failed: %s", exc)
        return []


# ── Stage B: image classification ─────────────────────────────────────────────

def _classify_output_images(image_paths: list[str]) -> list[str]:
    """
    Run IMAGE role classification on notebook output images.
    Reuses classify_figures helpers for consistency with academic pipeline.
    """
    from academic.management.commands.classify_figures import (
        _find_closest_type,
        _parse_relevance_response,
    )

    detected = set()
    for img_path in image_paths[:_MAX_OUTPUT_IMAGES]:
        if not Path(img_path).exists():
            continue
        try:
            rel = call_llm(
                role="IMAGE",
                prompt=RELEVANCE_PROMPT,
                image_path=img_path,
                system_prompt=RELEVANCE_SYSTEM_PROMPT,
            )
            is_vis, _ = _parse_relevance_response(str(rel))
            if not is_vis:
                continue
        except Exception:
            continue
        try:
            type_response = call_llm(
                role="IMAGE",
                prompt=type_classification_prompt(VIS_TYPES),
                image_path=img_path,
                response_format="json",
                system_prompt=TYPE_CLASSIFICATION_SYSTEM_PROMPT,
            )
            if isinstance(type_response, dict):
                vis_type = type_response.get("type", "")
            else:
                vis_type = ""
            if vis_type and vis_type not in VIS_TYPES:
                vis_type = _find_closest_type(vis_type)
            if vis_type:
                detected.add(vis_type)
        except Exception:
            continue

    return sorted(detected)


# ── Notebook parsing ───────────────────────────────────────────────────────────

def _parse_notebook(raw: str, path: Path) -> tuple[list[str], list[str]]:
    """
    Parse a Jupyter notebook JSON.
    Returns (code_cells_text, output_image_paths).
    """
    try:
        nb = json.loads(raw)
    except json.JSONDecodeError:
        return [raw], []

    code_cells: list[str] = []
    output_image_paths: list[str] = []

    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue

        source = cell.get("source", [])
        code_cells.append("".join(source) if isinstance(source, list) else str(source))

        for output in cell.get("outputs", []):
            if output.get("output_type") not in ("display_data", "execute_result"):
                continue
            for mime, content in output.get("data", {}).items():
                if not mime.startswith("image/"):
                    continue
                import base64
                from django.conf import settings
                ext = mime.split("/")[-1].replace("jpeg", "jpg")
                img_dir = Path(settings.MEDIA_ROOT) / "repos" / "output_imgs"
                img_dir.mkdir(parents=True, exist_ok=True)
                img_name = f"{path.stem}_out_{len(output_image_paths)}.{ext}"
                img_path = img_dir / img_name
                if isinstance(content, list):
                    content = "".join(content)
                try:
                    img_path.write_bytes(base64.b64decode(content))
                    output_image_paths.append(str(img_path))
                except Exception:
                    pass

    return code_cells, output_image_paths