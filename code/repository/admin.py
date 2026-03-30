from django.contrib import admin
from .models import RepoArtifact, RepoSource


@admin.register(RepoSource)
class RepoSourceAdmin(admin.ModelAdmin):
    list_display = ["title_short", "platform", "author", "language",
                    "stars", "last_updated", "artifact_count", "crawled_at"]
    list_filter = ["platform", "language"]
    search_fields = ["title", "author", "source_id"]
    readonly_fields = ["crawled_at"]

    def title_short(self, obj):
        return obj.title[:70] + "…" if len(obj.title) > 70 else obj.title
    title_short.short_description = "Title"

    def artifact_count(self, obj):
        return obj.artifacts.count()
    artifact_count.short_description = "Artifacts"


@admin.register(RepoArtifact)
class RepoArtifactAdmin(admin.ModelAdmin):
    list_display = ["id", "source_title_short", "artifact_type",
                    "detected_libraries_display", "detected_types_display",
                    "detection_method", "processed_at"]
    list_filter = ["artifact_type", "detection_method", "source__platform"]
    search_fields = ["source__title", "detected_chart_types", "detected_libraries"]
    readonly_fields = ["processed_at"]

    def source_title_short(self, obj):
        return str(obj.source)[:60]
    source_title_short.short_description = "Source"

    def detected_libraries_display(self, obj):
        libs = obj.get_detected_libraries()
        return ", ".join(libs[:4]) + ("…" if len(libs) > 4 else "")
    detected_libraries_display.short_description = "Libraries"

    def detected_types_display(self, obj):
        types = obj.get_detected_chart_types()
        return ", ".join(types[:4]) + ("…" if len(types) > 4 else "")
    detected_types_display.short_description = "Chart types"
