from django.contrib import admin
from django.utils.html import format_html

from .models import Paper, PaperFigure


@admin.register(Paper)
class PaperAdmin(admin.ModelAdmin):
    list_display = ["title_short", "year", "track", "source", "figure_count",
                    "has_pdf", "has_keywords", "ingested_at"]
    list_filter = ["source", "year", "track"]
    search_fields = ["title", "doi", "abstract"]
    readonly_fields = ["ingested_at"]
    ordering = ["-year", "title"]

    def title_short(self, obj):
        return obj.title[:70] + "…" if len(obj.title) > 70 else obj.title
    title_short.short_description = "Title"

    def figure_count(self, obj):
        return obj.figures.count()
    figure_count.short_description = "Figures"

    def has_pdf(self, obj):
        return bool(obj.pdf_local_path)
    has_pdf.boolean = True
    has_pdf.short_description = "PDF?"

    def has_keywords(self, obj):
        return bool(obj.keywords_extracted and obj.keywords_extracted != "[]")
    has_keywords.boolean = True
    has_keywords.short_description = "Keywords?"


@admin.register(PaperFigure)
class PaperFigureAdmin(admin.ModelAdmin):
    list_display = ["id", "paper_title_short", "figure_index", "is_visualization",
                    "vis_type", "vis_type_confidence", "annotation_source", "thumbnail"]
    list_filter = ["is_visualization", "vis_type", "annotation_source",
                   "paper__year", "paper__track"]
    search_fields = ["paper__title", "vis_type"]
    readonly_fields = ["extracted_at", "thumbnail_large"]
    ordering = ["paper", "figure_index"]

    def paper_title_short(self, obj):
        return str(obj.paper)[:60]
    paper_title_short.short_description = "Paper"

    def thumbnail(self, obj):
        if obj.image_url:
            return format_html(
                '<img src="{}" style="max-height:60px; max-width:80px;" />', obj.image_url
            )
        return "—"
    thumbnail.short_description = "Fig"

    def thumbnail_large(self, obj):
        if obj.image_url:
            return format_html(
                '<img src="{}" style="max-width:400px;" />', obj.image_url
            )
        return "—"
    thumbnail_large.short_description = "Figure preview"
