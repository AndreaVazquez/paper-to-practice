"""
Academic corpus models.

papers         — one record per IEEE VIS paper
paper_figures  — one record per extracted/imported figure image
"""

import json
from django.db import models


class Paper(models.Model):
    SOURCE_CHOICES = [
        ("visimages", "VisImages dataset"),
        ("vis2019", "IEEE VIS 2019"),
        ("vis2020", "IEEE VIS 2020"),
        ("vis2021", "IEEE VIS 2021"),
        ("vis2022", "IEEE VIS 2022"),
        ("vis2023", "IEEE VIS 2023"),
        ("vis2024", "IEEE VIS 2024"),
        ("vis2025", "IEEE VIS 2025"),
        ("seed_doi", "Seed DOI list"),
    ]
    TRACK_CHOICES = [
        ("InfoVis", "InfoVis"),
        ("VAST", "VAST"),
        ("SciVis", "SciVis"),
        ("unknown", "Unknown"),
    ]

    source = models.CharField(max_length=32, choices=SOURCE_CHOICES, db_index=True)
    doi = models.CharField(max_length=256, unique=True, null=True, blank=True)
    title = models.TextField()
    authors = models.TextField(default="[]")       # JSON array of author strings
    year = models.IntegerField(null=True, blank=True, db_index=True)
    track = models.CharField(
        max_length=16, choices=TRACK_CHOICES, default="unknown", db_index=True
    )
    abstract = models.TextField(blank=True, default="")
    pdf_url = models.URLField(max_length=1024, blank=True, default="")
    pdf_local_path = models.CharField(max_length=512, blank=True, default="")
    keywords_extracted = models.TextField(default="[]")  # JSON array, TEXT agent
    topics_extracted = models.TextField(default="[]")    # JSON array, TEXT agent
    ingested_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "papers"
        ordering = ["-year", "title"]

    def __str__(self) -> str:
        return f"[{self.year}] {self.title[:80]}"

    # ── JSON field helpers ─────────────────────────────────────────────────────

    def get_authors(self) -> list[str]:
        try:
            return json.loads(self.authors)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_authors(self, authors: list[str]) -> None:
        self.authors = json.dumps(authors)

    def get_keywords(self) -> list[str]:
        try:
            return json.loads(self.keywords_extracted)
        except (json.JSONDecodeError, TypeError):
            return []

    def get_topics(self) -> list[str]:
        try:
            return json.loads(self.topics_extracted)
        except (json.JSONDecodeError, TypeError):
            return []


class PaperFigure(models.Model):
    ANNOTATION_SOURCE_CHOICES = [
        ("visimages_json", "VisImages annotation JSON"),
        ("llm_classified", "LLM classified"),
        ("manual", "Manually annotated"),
    ]

    paper = models.ForeignKey(
        Paper, on_delete=models.CASCADE, related_name="figures"
    )
    figure_index = models.IntegerField()          # position in PDF (page-based)
    image_local_path = models.CharField(max_length=512)
    is_visualization = models.BooleanField(null=True, blank=True)  # NULL = unclassified
    vis_type = models.CharField(max_length=64, blank=True, default="", db_index=True)
    vis_type_confidence = models.FloatField(null=True, blank=True)
    vis_type_raw = models.TextField(blank=True, default="")  # full JSON response
    annotation_source = models.CharField(
        max_length=32,
        choices=ANNOTATION_SOURCE_CHOICES,
        blank=True,
        default="",
    )
    gemini_file_id = models.CharField(max_length=512, blank=True, default="")
    extracted_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "paper_figures"
        ordering = ["paper", "figure_index"]
        unique_together = [("paper", "figure_index")]

    def __str__(self) -> str:
        return f"Figure {self.figure_index} of {self.paper}"

    @property
    def image_url(self) -> str:
        """Return a URL-friendly path relative to MEDIA_ROOT.

        image_local_path is stored relative to MEDIA_ROOT (e.g.
        'visimages/images/1018/0.png' or 'papers/figures/paper_3_p001_i000.png').
        Absolute paths are also handled for backwards compatibility.
        """
        from django.conf import settings
        from pathlib import Path as _Path
        if not self.image_local_path:
            return ""
        try:
            p = _Path(self.image_local_path)
            if p.is_absolute():
                rel = str(p.relative_to(settings.MEDIA_ROOT)).replace("\\", "/")
            else:
                rel = self.image_local_path.replace("\\", "/")
            return settings.MEDIA_URL + rel
        except ValueError:
            return ""