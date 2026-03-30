from django.contrib import admin
from .models import DriftAnnotation, Narrative, NarrativeQuery, Trace


@admin.register(Trace)
class TraceAdmin(admin.ModelAdmin):
    list_display = ["id", "figure_info", "artifact_info", "match_method",
                    "match_confidence", "verified", "has_drift", "created_at"]
    list_filter = ["match_method", "verified"]
    search_fields = ["figure__vis_type", "artifact__source__title"]
    readonly_fields = ["created_at"]
    actions = ["mark_verified"]

    def figure_info(self, obj):
        return f"Fig {obj.figure_id} ({obj.figure.vis_type})"
    figure_info.short_description = "Figure"

    def artifact_info(self, obj):
        return str(obj.artifact.source)[:50]
    artifact_info.short_description = "Artifact"

    def has_drift(self, obj):
        return hasattr(obj, "drift_annotation")
    has_drift.boolean = True
    has_drift.short_description = "Drift?"

    @admin.action(description="Mark selected traces as verified")
    def mark_verified(self, request, queryset):
        queryset.update(verified=True)


@admin.register(DriftAnnotation)
class DriftAnnotationAdmin(admin.ModelAdmin):
    list_display = ["id", "trace_id", "encoding_drift", "interaction_drift",
                    "task_drift", "annotated_by", "severity_score_display",
                    "annotated_at"]
    list_filter = ["encoding_drift", "interaction_drift", "task_drift", "annotated_by"]
    readonly_fields = ["annotated_at"]

    def severity_score_display(self, obj):
        return obj.severity_score()
    severity_score_display.short_description = "Severity"


@admin.register(Narrative)
class NarrativeAdmin(admin.ModelAdmin):
    list_display = ["id", "vis_type", "status", "view_count", "model_used",
                    "generated_at", "published_at"]
    list_filter = ["status", "vis_type"]
    search_fields = ["vis_type", "query_text"]
    readonly_fields = ["generated_at", "published_at", "view_count"]

    def narrative_preview(self, obj):
        excerpt = obj.get_text_excerpt()
        return excerpt[:120] + "…" if len(excerpt) > 120 else excerpt
    narrative_preview.short_description = "Preview"


@admin.register(NarrativeQuery)
class NarrativeQueryAdmin(admin.ModelAdmin):
    list_display = ["id", "vis_type", "query_preview", "timestamp", "narrative"]
    list_filter = ["vis_type"]
    search_fields = ["vis_type", "query_text"]
    readonly_fields = ["timestamp"]

    def query_preview(self, obj):
        return obj.query_text[:80] + "…" if len(obj.query_text) > 80 else obj.query_text
    query_preview.short_description = "Query"
