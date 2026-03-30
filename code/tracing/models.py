"""
Bridge (Tracing) models.

traces           — links a PaperFigure to a RepoArtifact by shared chart type
drift_annotations — encoding/interaction/task drift per trace
narratives       — design anchor narrative per academic figure
"""

import json
from django.db import models

from academic.models import PaperFigure
from repository.models import RepoArtifact


class Trace(models.Model):
    MATCH_METHOD_CHOICES = [
        ("chart_type_match", "Chart Type Match"),
        ("keyword_match", "Keyword Match"),
    ]
    ANNOTATION_STATUS_CHOICES = [
        ("unannotated", "Unannotated"),
        ("annotated", "Annotated"),
        # The figure image was inspected by Gemini and judged not to actually
        # show the claimed vis_type — drift annotation was skipped.
        ("invalid", "Invalid — figure type unconfirmed"),
    ]

    figure = models.ForeignKey(
        PaperFigure, on_delete=models.CASCADE, related_name="traces"
    )
    artifact = models.ForeignKey(
        RepoArtifact, on_delete=models.CASCADE, related_name="traces"
    )
    match_method = models.CharField(max_length=32, choices=MATCH_METHOD_CHOICES,
                                    default="chart_type_match")
    match_confidence = models.FloatField(default=1.0)
    verified = models.BooleanField(default=False, db_index=True)
    annotation_status = models.CharField(
        max_length=12,
        choices=ANNOTATION_STATUS_CHOICES,
        default="unannotated",
        db_index=True,
    )
    invalid_reason = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "traces"
        unique_together = [("figure", "artifact")]
        ordering = ["-match_confidence", "created_at"]

    def __str__(self) -> str:
        return (
            f"Trace: Figure {self.figure_id} ↔ Artifact {self.artifact_id} "
            f"({self.match_confidence:.2f})"
        )


class DriftAnnotation(models.Model):
    DRIFT_CHOICES = [
        ("none", "None"),
        ("minor", "Minor"),
        ("major", "Major"),
    ]
    ANNOTATED_BY_CHOICES = [
        ("llm", "LLM"),
        ("manual", "Manual"),
    ]

    trace = models.OneToOneField(
        Trace, on_delete=models.CASCADE, related_name="drift_annotation"
    )
    encoding_drift = models.CharField(max_length=8, choices=DRIFT_CHOICES, default="none")
    interaction_drift = models.CharField(max_length=8, choices=DRIFT_CHOICES, default="none")
    task_drift = models.CharField(max_length=8, choices=DRIFT_CHOICES, default="none")
    encoding_notes = models.TextField(blank=True, default="")
    interaction_notes = models.TextField(blank=True, default="")
    task_notes = models.TextField(blank=True, default="")
    annotated_by = models.CharField(max_length=8, choices=ANNOTATED_BY_CHOICES, default="llm")
    annotated_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "drift_annotations"
        ordering = ["-annotated_at"]

    def __str__(self) -> str:
        return (
            f"Drift for Trace {self.trace_id}: "
            f"enc={self.encoding_drift}, "
            f"inter={self.interaction_drift}, "
            f"task={self.task_drift}"
        )

    def severity_score(self) -> int:
        """Numeric severity: none=0, minor=1, major=2. Sum across three dimensions."""
        _map = {"none": 0, "minor": 1, "major": 2}
        return (
            _map.get(self.encoding_drift, 0)
            + _map.get(self.interaction_drift, 0)
            + _map.get(self.task_drift, 0)
        )


class Narrative(models.Model):
    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("published", "Published"),
    ]

    vis_type = models.CharField(max_length=64, db_index=True)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default="draft", db_index=True)
    blocks = models.TextField(default="[]")
    # Block schema — each element is a JSON object with a stable `uuid` (str).
    # Four block types are defined; publisher.py and author.html must stay in sync:
    #
    #   Text block:
    #     { "uuid": "...", "type": "text", "content": "<narrative prose>" }
    #
    #   Chart block:
    #     { "uuid": "...", "type": "chart",
    #       "prompt": "<NL description used to generate>",
    #       "plotly_spec": { "data": [...], "layout": {...} } }
    #
    #   Figures block (one per narrative, lists academic source figures):
    #     { "uuid": "...", "type": "figures",
    #       "figure_ids": [int, ...],
    #       "metadata": [{ "id", "title", "year", "doi", "vis_type",
    #                       "image_local_path" }, ...] }
    #
    #   Notebooks block (one per narrative, lists repository sources):
    #     { "uuid": "...", "type": "notebooks",
    #       "artifact_ids": [int, ...],
    #       "metadata": [{ "id", "platform", "title", "url",
    #                       "stars", "chart_types": [str] }, ...] }
    #
    # Block UUIDs are the stable identity for all edit operations (delete,
    # reorder, regen-chart). Index-based addressing is never used.
    query_text = models.TextField(blank=True, default="")
    source_figures = models.TextField(default="[]")   # JSON array of figure IDs
    source_artifacts = models.TextField(default="[]") # JSON array of artifact IDs
    view_count = models.IntegerField(default=0)
    html_path = models.CharField(max_length=512, blank=True, default="")
    pdf_path = models.CharField(max_length=512, blank=True, default="")
    json_ld_path = models.CharField(max_length=512, blank=True, default="")
    generated_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True)
    model_used = models.CharField(max_length=128, blank=True, default="")

    class Meta:
        db_table = "narratives"
        ordering = ["-view_count", "-generated_at"]

    def __str__(self) -> str:
        return f"Narrative: {self.vis_type} [{self.status}]"

    def get_blocks(self) -> list:
        try:
            return json.loads(self.blocks)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_blocks(self, blocks: list) -> None:
        self.blocks = json.dumps(blocks)

    def get_source_figures(self) -> list[int]:
        try:
            return json.loads(self.source_figures)
        except (json.JSONDecodeError, TypeError):
            return []

    def get_source_artifacts(self) -> list[int]:
        try:
            return json.loads(self.source_artifacts)
        except (json.JSONDecodeError, TypeError):
            return []

    def get_text_excerpt(self) -> str:
        """Return first sentence of the first text block, for gallery cards."""
        for block in self.get_blocks():
            if block.get("type") == "text":
                text = block.get("content", "")
                end = text.find(". ")
                return text[: end + 1] if end != -1 else text[:160]
        return ""


class NarrativeQuery(models.Model):
    """Logs every query submitted to the authoring similarity check.

    Interest is recorded even if the user never proceeds to generation.
    The narrative FK is filled in when the query leads to a published narrative.
    """
    vis_type = models.CharField(max_length=64, db_index=True)
    query_text = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    narrative = models.ForeignKey(
        Narrative,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="queries",
    )

    class Meta:
        db_table = "narrative_queries"
        ordering = ["-timestamp"]

    def __str__(self) -> str:
        return f"Query [{self.vis_type}] {self.query_text[:60]}"