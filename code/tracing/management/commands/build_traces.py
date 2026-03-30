"""
Component 7 — Trace Construction
Command: python manage.py build_traces [--dry-run]

Builds trace links between academic figures and repository artifacts using a
representative sampling strategy — not an exhaustive cartesian product.

Hard cap: FIGURES_PER_TYPE (5) × ARTIFACTS_PER_TYPE (9) = 45 traces per vis_type.

── Figure selection ──────────────────────────────────────────────────────────
Figures are split into two era buckets:
  old — annotation_source="visimages_json"  (VisImages, effectively pre-2020)
  new — annotation_source="llm_classified"  (VIS2020–2025)

Guaranteed minimums: 2 old + 2 new. The 5th slot goes to whichever bucket
has more candidates. If a bucket has fewer than 2, the freed slots spill to
the other bucket.

Within each bucket figures are selected by _era_figures():
  - Max 1 figure per paper.
  - Year-spread within the bucket: don't take a second figure from the same
    year until every other year in that bucket has one figure. This prevents
    clustering at e.g. 2023/2024 just because they have the most papers.
  - Within a year, pick by confidence descending.

Only fresh figures are eligible — figures already in a non-invalid trace for
this vis_type are excluded so each re-run adds new evidence rather than
duplicating existing pairs.

Figures permanently marked invalid by annotate_drift are excluded globally.

── Artifact selection ────────────────────────────────────────────────────────
ARTIFACTS_PER_TYPE (9) artifacts are selected with guaranteed platform slots:
  - 3 guaranteed for Kaggle (if available)
  - 3 guaranteed for GitHub (if available)
  - 3 guaranteed for Observable HQ (if available)
  Freed slots from under-represented platforms spill into a fill pass.

All artifacts are ranked by stars / n_detected_types (specificity score) so
focused notebooks rank above generic dashboards at the same star count.

Artifacts are always re-selected fresh from the full ranked pool on every run
— no memory of what was previously paired. The same top-9 will re-appear with
new figures, which is intentional: the artifact pool is the stable reference
frame; figure diversity is what grows across runs.

── Re-run / budget logic ────────────────────────────────────────────────────
On each run, for a vis_type:
  1. Count existing non-invalid traces. If already at 45, skip.
  2. remaining = 45 − existing_non_invalid.
  3. Pick up to FIGURES_PER_TYPE fresh figures (era-bucket logic above).
     If fewer fresh figures are available than the full 5, take what exists.
  4. Select the standard 9 platform-balanced artifacts.
  5. Create traces for all (fresh_fig × artifact) pairs, capped at remaining.
     The DB unique_together constraint silently handles any pre-existing pairs.

── Confidence ────────────────────────────────────────────────────────────────
  1.0 — exact vis_type match    (auto-verified, eligible for annotation)
  0.7 — same parent category    (not auto-verified)
  0.0 — no match                (trace not created)

Idempotent: skips (figure, artifact) pairs already in the Trace table.
"""

import logging
from collections import defaultdict

from django.core.management.base import BaseCommand

from academic.models import PaperFigure
from core.agent_log import emit
from core.taxonomy import get_category
from repository.models import RepoArtifact
from tracing.models import Trace

logger = logging.getLogger(__name__)

AUTO_VERIFY_THRESHOLD = 0.8

# Hard cap: 5 figures × 9 artifacts = 45 traces per vis_type.
FIGURES_PER_TYPE   = 5
ARTIFACTS_PER_TYPE = 9
MAX_TRACES_PER_TYPE = FIGURES_PER_TYPE * ARTIFACTS_PER_TYPE  # 45

# Guaranteed artifact slots per platform. Three platforms × 3 = 9 total.
PLATFORM_GUARANTEE = 3
PLATFORMS = ("kaggle", "github", "observablehq")

# Era split: figures with annotation_source in OLD_SOURCES go to the old bucket.
OLD_SOURCES = {"visimages_json"}

# Minimum guaranteed figures from each era bucket.
# The 5th slot goes to whichever bucket has more candidates.
ERA_MIN_OLD = 2
ERA_MIN_NEW = 2


class Command(BaseCommand):
    help = (
        "Build representative trace links between paper figures and repository "
        f"artifacts. Hard cap: {MAX_TRACES_PER_TYPE} traces per vis_type "
        f"({FIGURES_PER_TYPE} era-balanced figures × "
        f"{ARTIFACTS_PER_TYPE} platform-balanced artifacts)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print the sampling plan per vis_type without writing to the DB.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        emit(
            agent="build_traces",
            status="started",
            message=(
                f"Building representative traces "
                f"(cap={MAX_TRACES_PER_TYPE}/type, dry_run={dry_run})"
            ),
        )

        # ── Load figures ───────────────────────────────────────────────────────
        # Order by confidence desc so within each year the best figure is first.
        all_figures = list(
            PaperFigure.objects
            .filter(is_visualization=True)
            .exclude(vis_type="")
            .exclude(vis_type__isnull=True)
            .select_related("paper")
            .order_by("-vis_type_confidence", "id")
        )

        # Globally exclude figures already judged invalid by annotate_drift.
        invalid_figure_ids: set[int] = set(
            Trace.objects.filter(annotation_status="invalid")
            .values_list("figure_id", flat=True)
            .distinct()
        )
        if invalid_figure_ids:
            emit(
                agent="build_traces", status="running",
                message=(
                    f"Excluding {len(invalid_figure_ids)} figures already marked "
                    f"invalid by annotate_drift."
                ),
            )

        figures_by_type: dict[str, list] = defaultdict(list)
        for fig in all_figures:
            if fig.id not in invalid_figure_ids:
                figures_by_type[fig.vis_type].append(fig)

        # ── Load artifacts ─────────────────────────────────────────────────────
        all_artifacts = list(
            RepoArtifact.objects
            .exclude(detected_chart_types__in=["[]", "", None])
            .select_related("source")
            .order_by("-source__stars", "id")
        )

        artifact_types_map: dict[int, list[str]] = {}
        artifacts_by_type: dict[str, list] = defaultdict(list)
        for art in all_artifacts:
            types = art.get_detected_chart_types()
            artifact_types_map[art.id] = types
            for t in types:
                artifacts_by_type[t].append(art)

        # Sort each type's list by specificity: stars / n_types.
        for vt, arts in artifacts_by_type.items():
            artifacts_by_type[vt] = sorted(
                arts,
                key=lambda a: (a.source.stars or 0) / max(len(artifact_types_map[a.id]), 1),
                reverse=True,
            )

        # ── Matched vis_types ──────────────────────────────────────────────────
        matched_types = sorted(
            t for t in figures_by_type if artifacts_by_type.get(t)
        )

        if not matched_types:
            emit(agent="build_traces", status="done",
                 message="No vis_types found with both figures and artifacts.")
            self.stdout.write(self.style.WARNING("No matched vis_types found."))
            return

        emit(
            agent="build_traces", status="running",
            message=(
                f"Found {len(matched_types)} matched vis_types across "
                f"{len(all_figures)} figures and {len(all_artifacts)} artifacts"
            ),
        )

        created = 0
        skipped = 0
        type_summaries = []

        for type_idx, vis_type in enumerate(matched_types):
            figs = figures_by_type[vis_type]
            arts = artifacts_by_type[vis_type]

            # ── Budget check ───────────────────────────────────────────────────
            existing_non_invalid = (
                Trace.objects.filter(figure__vis_type=vis_type)
                .exclude(annotation_status="invalid")
                .count()
            )
            if existing_non_invalid >= MAX_TRACES_PER_TYPE:
                emit(
                    agent="build_traces", status="running",
                    message=(
                        f"{vis_type}: already at cap "
                        f"({existing_non_invalid}/{MAX_TRACES_PER_TYPE}) — skipping."
                    ),
                    progress=[type_idx + 1, len(matched_types)],
                )
                type_summaries.append({
                    "type": vis_type, "old_pool": 0, "new_pool": 0,
                    "total_arts": len(arts), "sampled_figs": 0,
                    "sampled_arts": 0, "planned": 0, "created": 0, "skipped": 0,
                })
                continue

            remaining = MAX_TRACES_PER_TYPE - existing_non_invalid

            # ── Fresh figure pool ──────────────────────────────────────────────
            already_paired: set[int] = set(
                Trace.objects.filter(figure__vis_type=vis_type)
                .exclude(annotation_status="invalid")
                .values_list("figure_id", flat=True)
                .distinct()
            )
            fresh_figs = [f for f in figs if f.id not in already_paired]

            # Split into era buckets
            old_pool = [f for f in fresh_figs if f.annotation_source in OLD_SOURCES]
            new_pool = [f for f in fresh_figs if f.annotation_source not in OLD_SOURCES]

            rep_figs = _select_era_figures(old_pool, new_pool, FIGURES_PER_TYPE)

            if not rep_figs:
                emit(
                    agent="build_traces", status="running",
                    message=(
                        f"{vis_type}: no fresh figures available "
                        f"(old_pool={len(old_pool)}, new_pool={len(new_pool)}) — skipping."
                    ),
                    progress=[type_idx + 1, len(matched_types)],
                )
                type_summaries.append({
                    "type": vis_type,
                    "old_pool": len(old_pool), "new_pool": len(new_pool),
                    "total_arts": len(arts), "sampled_figs": 0,
                    "sampled_arts": 0, "planned": 0, "created": 0, "skipped": 0,
                })
                continue

            # ── Platform-balanced artifact selection ───────────────────────────
            rep_arts = _select_platform_artifacts(arts, ARTIFACTS_PER_TYPE, PLATFORM_GUARANTEE)

            n_figs = len(rep_figs)
            n_arts = len(rep_arts)
            planned = min(n_figs * n_arts, remaining)

            summary = {
                "type":         vis_type,
                "old_pool":     len(old_pool),
                "new_pool":     len(new_pool),
                "total_arts":   len(arts),
                "sampled_figs": n_figs,
                "sampled_arts": n_arts,
                "planned":      planned,
                "created":      0,
                "skipped":      0,
            }

            # Emit a breakdown showing era and platform composition
            old_sel = sum(1 for f in rep_figs if f.annotation_source in OLD_SOURCES)
            new_sel = n_figs - old_sel
            platform_counts = {}
            for a in rep_arts:
                platform_counts[a.source.platform] = platform_counts.get(a.source.platform, 0) + 1
            platform_str = " · ".join(
                f"{p}={platform_counts.get(p, 0)}" for p in PLATFORMS
            )
            emit(
                agent="build_traces", status="running",
                message=(
                    f"{vis_type}: figs={n_figs} (old={old_sel} new={new_sel}) "
                    f"× arts={n_arts} ({platform_str}) = {planned} traces"
                ),
                progress=[type_idx + 1, len(matched_types)],
            )

            if not dry_run:
                traces_created_this_type = 0
                for fig in rep_figs:
                    fig_category = get_category(fig.vis_type)
                    for art in rep_arts:
                        if traces_created_this_type >= remaining:
                            break
                        art_types = artifact_types_map.get(art.id, [])
                        confidence = _compute_confidence(fig.vis_type, fig_category, art_types)
                        if confidence == 0.0:
                            continue
                        if Trace.objects.filter(figure=fig, artifact=art).exists():
                            skipped += 1
                            summary["skipped"] += 1
                            continue
                        Trace.objects.create(
                            figure=fig,
                            artifact=art,
                            match_method="chart_type_match",
                            match_confidence=confidence,
                            verified=(confidence >= AUTO_VERIFY_THRESHOLD),
                        )
                        created += 1
                        traces_created_this_type += 1
                        summary["created"] += 1
                    if traces_created_this_type >= remaining:
                        break

            type_summaries.append(summary)

        total_planned = sum(s["planned"] for s in type_summaries)

        # ── Summary table ──────────────────────────────────────────────────────
        header = (
            f"{'Vis Type':<28} {'Old':>5} {'New':>5} {'Arts':>6} "
            f"{'Figs':>5} {'ArtSel':>7} {'Planned':>8}"
        )
        rows = [
            f"{s['type']:<28} {s['old_pool']:>5} {s['new_pool']:>5} "
            f"{s['total_arts']:>6} {s['sampled_figs']:>5} {s['sampled_arts']:>7} "
            f"{s['planned']:>8}"
            for s in type_summaries
        ]
        table = "\n".join(
            [header, "-" * 72] + rows +
            ["-" * 72, f"{'TOTAL':<28} {'':>5} {'':>5} {'':>6} {'':>5} {'':>7} {total_planned:>8}"]
        )

        if dry_run:
            emit(
                agent="build_traces", status="done",
                message=(
                    f"DRY RUN: would create ~{total_planned} traces "
                    f"across {len(type_summaries)} vis_types"
                ),
            )
            self.stdout.write(f"\nDRY RUN — sampling plan:\n\n{table}\n")
            return

        emit(
            agent="build_traces", status="done",
            message=(
                f"Done. {created} traces created, {skipped} already existed — "
                f"across {len(type_summaries)} vis_types"
            ),
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"\nBuilt {created} traces ({skipped} already existed).\n\n{table}\n"
            )
        )


# ── Figure selection helpers ───────────────────────────────────────────────────

def _select_era_figures(old_pool: list, new_pool: list, target: int) -> list:
    """
    Select up to `target` figures with guaranteed representation from both eras.

    Guaranteed minimums: ERA_MIN_OLD from old, ERA_MIN_NEW from new.
    The remaining slot(s) go to whichever bucket has more candidates.
    If a bucket can't fill its minimum, freed slots spill to the other bucket.

    Within each bucket, _era_figures() applies year-spread (no two figures
    from the same year before every other year in that bucket has one) and
    paper diversity (max 1 per paper_id).
    """
    # Determine allocation
    old_target = ERA_MIN_OLD
    new_target = ERA_MIN_NEW
    spare = target - ERA_MIN_OLD - ERA_MIN_NEW  # = 1

    # Give the spare slot to the larger bucket
    if len(new_pool) >= len(old_pool):
        new_target += spare
    else:
        old_target += spare

    # Clamp to available pool sizes
    old_target = min(old_target, len(old_pool))
    new_target = min(new_target, len(new_pool))

    # If a bucket is under its minimum, spill freed slots to the other
    old_deficit = ERA_MIN_OLD - old_target  # > 0 if old is short
    new_deficit = ERA_MIN_NEW - new_target
    if old_deficit > 0:
        new_target = min(new_target + old_deficit, len(new_pool))
    if new_deficit > 0:
        old_target = min(old_target + new_deficit, len(old_pool))

    old_selected = _era_figures(old_pool, old_target)
    new_selected = _era_figures(new_pool, new_target)
    return old_selected + new_selected


def _era_figures(pool: list, target: int) -> list:
    """
    Select up to `target` figures from a single era bucket.

    Rules:
      - Max 1 figure per paper_id.
      - Year-spread: round-robin through distinct years in ascending order.
        Before any year gets a second figure, every other year must have one.
      - Within a year, figures are already sorted by confidence desc (from the
        global query ordering), so the first eligible figure per year is best.
    """
    if not pool or target <= 0:
        return []

    by_year: dict[int, list] = defaultdict(list)
    for fig in pool:
        by_year[fig.paper.year or 0].append(fig)

    sorted_years = sorted(by_year.keys())
    year_pos = {y: 0 for y in sorted_years}
    paper_counts: dict[int, int] = {}
    selected: list = []

    while len(selected) < target:
        made_progress = False
        for year in sorted_years:
            if len(selected) >= target:
                break
            year_figs = by_year[year]
            while year_pos[year] < len(year_figs):
                fig = year_figs[year_pos[year]]
                year_pos[year] += 1
                if paper_counts.get(fig.paper_id, 0) < 1:
                    selected.append(fig)
                    paper_counts[fig.paper_id] = 1
                    made_progress = True
                    break
        if not made_progress:
            break

    return selected


# ── Artifact selection helpers ─────────────────────────────────────────────────

def _select_platform_artifacts(
    arts: list,
    total: int,
    guarantee: int,
) -> list:
    """
    Select up to `total` artifacts with `guarantee` guaranteed slots per platform.

    Phase 1 — guaranteed: for each platform in PLATFORMS, take the top
    `guarantee` artifacts by specificity score (already sorted in `arts`).

    Phase 2 — fill: remaining slots filled from the full `arts` list
    (stars/n_types ranked), skipping already-selected artifacts.

    Freed slots from platforms with fewer than `guarantee` artifacts spill
    into the fill phase rather than being wasted.
    """
    selected: list = []
    selected_ids: set[int] = set()

    # Phase 1 — guaranteed slots per platform
    for platform in PLATFORMS:
        count = 0
        for art in arts:
            if count >= guarantee:
                break
            if art.source.platform == platform and art.id not in selected_ids:
                selected.append(art)
                selected_ids.add(art.id)
                count += 1

    # Phase 2 — fill remaining capacity
    remaining = total - len(selected)
    for art in arts:
        if remaining <= 0:
            break
        if art.id not in selected_ids:
            selected.append(art)
            selected_ids.add(art.id)
            remaining -= 1

    return selected


# ── Confidence ─────────────────────────────────────────────────────────────────

def _compute_confidence(fig_type: str, fig_category: str | None, art_types: list[str]) -> float:
    """1.0 = exact match, 0.7 = same parent category, 0.0 = no match."""
    if fig_type in art_types:
        return 1.0
    if fig_category:
        for art_type in art_types:
            art_cat = get_category(art_type)
            if art_cat and art_cat == fig_category:
                return 0.7
    return 0.0