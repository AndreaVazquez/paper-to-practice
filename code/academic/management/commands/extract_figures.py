"""
Component 2 — Figure Extraction
Command: python manage.py extract_figures [--paper-id N] [--limit N]

Opens PDFs with PyMuPDF, extracts embedded images above a minimum size,
saves them to media/papers/figures/, creates PaperFigure records.

Idempotent: skips papers that already have figures.
"""

import logging
from pathlib import Path

import fitz  # PyMuPDF
from django.conf import settings
from django.core.management.base import BaseCommand

from academic.models import Paper, PaperFigure
from core.agent_log import emit

logger = logging.getLogger(__name__)

# Minimum image dimensions — ignore tiny icons, logos, decorations
MIN_WIDTH = 200
MIN_HEIGHT = 200

FIGURES_DIR = Path(settings.MEDIA_ROOT) / "papers" / "figures"


class Command(BaseCommand):
    help = "Extract figures from downloaded paper PDFs."

    def add_arguments(self, parser):
        parser.add_argument("--paper-id", type=int, default=None,
                            help="Only process a single paper by ID")
        parser.add_argument("--limit", type=int, default=None,
                            help="Stop after processing N papers")

    def handle(self, *args, **options):
        FIGURES_DIR.mkdir(parents=True, exist_ok=True)

        # Target: papers with a local PDF but no figures yet.
        # annotate+filter is explicit and avoids unexpected behaviour from
        # exclude(figures__isnull=False) with reverse FK lookups.
        from django.db.models import Count
        qs = (
            Paper.objects.filter(pdf_local_path__gt="")
            .annotate(fig_count=Count("figures"))
            .filter(fig_count=0)
        )
        if options["paper_id"]:
            qs = qs.filter(id=options["paper_id"])

        papers = list(qs)
        if options["limit"]:
            papers = papers[: options["limit"]]

        total = len(papers)
        emit(agent="extract_figures", status="started",
             message=f"Extracting figures from {total} PDFs")

        extracted_total = 0
        for i, paper in enumerate(papers):
            n = self._extract_from_paper(paper)
            extracted_total += n
            emit(agent="extract_figures", status="running",
                 message=f"Paper {paper.id} ({paper.year}): {n} figures extracted",
                 record_id=paper.id, progress=[i + 1, total])

        emit(agent="extract_figures", status="done",
             message=f"Done. {extracted_total} figures extracted from {total} PDFs")
        self.stdout.write(
            self.style.SUCCESS(f"Extracted {extracted_total} figures from {total} PDFs")
        )

    def _extract_from_paper(self, paper: Paper) -> int:
        # pdf_local_path is stored relative to MEDIA_ROOT; resolve to absolute.
        _raw = paper.pdf_local_path
        pdf_path = Path(_raw) if Path(_raw).is_absolute() else Path(settings.MEDIA_ROOT) / _raw
        if not pdf_path.exists():
            emit(agent="extract_figures", status="skipped",
                 message=f"PDF not found: {pdf_path}", level="warning")
            return 0

        extracted = 0
        try:
            doc = fitz.open(str(pdf_path))
        except Exception as exc:
            emit(agent="extract_figures", status="error",
                 message=f"Cannot open PDF {pdf_path.name}: {exc}", level="error")
            return 0

        figure_index = 0
        with doc:
            for page_num in range(len(doc)):
                page = doc[page_num]
                images = page.get_images(full=True)

                for img_index, img_info in enumerate(images):
                    xref = img_info[0]
                    try:
                        base_image = doc.extract_image(xref)
                    except Exception:
                        continue

                    width = base_image.get("width", 0)
                    height = base_image.get("height", 0)

                    if width < MIN_WIDTH or height < MIN_HEIGHT:
                        continue  # Skip tiny images

                    ext = base_image.get("ext", "png")
                    img_bytes = base_image["image"]

                    # Save image file
                    filename = f"paper_{paper.id}_p{page_num:03d}_i{img_index:03d}.{ext}"
                    out_path = FIGURES_DIR / filename

                    out_path.write_bytes(img_bytes)

                    # Create DB record — store path relative to MEDIA_ROOT
                    PaperFigure.objects.get_or_create(
                        paper=paper,
                        figure_index=figure_index,
                        defaults={
                            "image_local_path": str(out_path.relative_to(settings.MEDIA_ROOT)),
                            "is_visualization": None,  # unclassified
                            "annotation_source": "",
                        },
                    )
                    figure_index += 1
                    extracted += 1

        return extracted