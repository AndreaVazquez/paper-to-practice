"""
Repository corpus models.

repo_sources   — one record per Kaggle notebook / GitHub repo
repo_artifacts — one record per downloadable artifact (notebook, script)
"""

import json
from django.db import models


class RepoSource(models.Model):
    PLATFORM_CHOICES = [
        ("kaggle", "Kaggle"),
        ("github", "GitHub"),
        ("observablehq", "ObservableHQ"),
    ]

    platform = models.CharField(max_length=16, choices=PLATFORM_CHOICES, db_index=True)
    source_id = models.CharField(max_length=256, unique=True)  # kaggle kernel ref or github full_name
    url = models.URLField(max_length=1024)
    title = models.TextField()
    author = models.CharField(max_length=256, blank=True, default="")
    stars = models.IntegerField(null=True, blank=True)  # kaggle votes or github stars
    language = models.CharField(max_length=32, blank=True, default="")
    last_updated = models.DateField(null=True, blank=True)
    crawled_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "repo_sources"
        ordering = ["-stars", "platform"]

    def __str__(self) -> str:
        return f"[{self.platform}] {self.title[:60]}"


class RepoArtifact(models.Model):
    ARTIFACT_TYPE_CHOICES = [
        ("notebook", "Jupyter Notebook"),
        ("script", "Python Script"),
        ("markdown", "Markdown"),
    ]
    DETECTION_METHOD_CHOICES = [
        ("code_analysis", "Code Analysis"),
        ("image_classification", "Image Classification"),
        ("both", "Both"),
    ]

    source = models.ForeignKey(
        RepoSource, on_delete=models.CASCADE, related_name="artifacts"
    )
    artifact_type = models.CharField(max_length=16, choices=ARTIFACT_TYPE_CHOICES,
                                     default="notebook")
    raw_content_path = models.CharField(max_length=512, blank=True, default="")
    detected_libraries = models.TextField(default="[]")    # JSON array
    detected_chart_types = models.TextField(default="[]")  # JSON array
    detection_method = models.CharField(
        max_length=32, choices=DETECTION_METHOD_CHOICES, blank=True, default=""
    )
    output_images_paths = models.TextField(default="[]")   # JSON array of local paths
    processed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "repo_artifacts"
        ordering = ["source", "artifact_type"]

    def __str__(self) -> str:
        return f"Artifact ({self.artifact_type}) of {self.source}"

    # ── JSON field helpers ─────────────────────────────────────────────────────

    def get_detected_libraries(self) -> list[str]:
        try:
            return json.loads(self.detected_libraries)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_detected_libraries(self, libs: list[str]) -> None:
        self.detected_libraries = json.dumps(sorted(set(libs)))

    def get_detected_chart_types(self) -> list[str]:
        try:
            return json.loads(self.detected_chart_types)
        except (json.JSONDecodeError, TypeError):
            return []

    def set_detected_chart_types(self, types: list[str]) -> None:
        self.detected_chart_types = json.dumps(sorted(set(types)))

    def get_output_images(self) -> list[str]:
        try:
            return json.loads(self.output_images_paths)
        except (json.JSONDecodeError, TypeError):
            return []