# Drops and recreates the narratives table (0 rows, no data to preserve).
# Adds the narrative_queries table.

import django.db.models.deletion
import django.utils.timezone
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("tracing", "0001_initial"),
    ]

    operations = [
        # ── Drop old Narrative (0 rows, safe) ──────────────────────────────────
        migrations.DeleteModel(name="Narrative"),

        # ── Recreate Narrative ─────────────────────────────────────────────────
        migrations.CreateModel(
            name="Narrative",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True,
                                           serialize=False, verbose_name="ID")),
                ("vis_type", models.CharField(db_index=True, max_length=64, unique=True)),
                ("status", models.CharField(
                    choices=[("draft", "Draft"), ("published", "Published")],
                    db_index=True, default="draft", max_length=16,
                )),
                ("blocks", models.TextField(default="[]")),
                ("query_text", models.TextField(blank=True, default="")),
                ("source_figures", models.TextField(default="[]")),
                ("source_artifacts", models.TextField(default="[]")),
                ("view_count", models.IntegerField(default=0)),
                ("html_path", models.CharField(blank=True, default="", max_length=512)),
                ("pdf_path", models.CharField(blank=True, default="", max_length=512)),
                ("json_ld_path", models.CharField(blank=True, default="", max_length=512)),
                ("generated_at", models.DateTimeField(auto_now_add=True)),
                ("published_at", models.DateTimeField(blank=True, null=True)),
                ("model_used", models.CharField(blank=True, default="", max_length=128)),
            ],
            options={
                "db_table": "narratives",
                "ordering": ["-view_count", "-generated_at"],
            },
        ),

        # ── NarrativeQuery ─────────────────────────────────────────────────────
        migrations.CreateModel(
            name="NarrativeQuery",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True,
                                           serialize=False, verbose_name="ID")),
                ("vis_type", models.CharField(db_index=True, max_length=64)),
                ("query_text", models.TextField()),
                ("timestamp", models.DateTimeField(auto_now_add=True)),
                ("narrative", models.ForeignKey(
                    blank=True, null=True,
                    on_delete=django.db.models.deletion.SET_NULL,
                    related_name="queries",
                    to="tracing.narrative",
                )),
            ],
            options={
                "db_table": "narrative_queries",
                "ordering": ["-timestamp"],
            },
        ),
    ]
