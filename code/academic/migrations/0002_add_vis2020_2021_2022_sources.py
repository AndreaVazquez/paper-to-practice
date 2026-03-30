"""
Add vis2020, vis2021, vis2022 to Paper.source choices.

SQLite does not enforce CharField choices at the DB level, so no column
change is needed — this migration purely updates Django's field metadata
so the admin, forms, and serialisers reflect the new values.
"""

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("academic", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="paper",
            name="source",
            field=models.CharField(
                choices=[
                    ("visimages", "VisImages dataset"),
                    ("vis2020", "IEEE VIS 2020"),
                    ("vis2021", "IEEE VIS 2021"),
                    ("vis2022", "IEEE VIS 2022"),
                    ("vis2023", "IEEE VIS 2023"),
                    ("vis2024", "IEEE VIS 2024"),
                    ("vis2025", "IEEE VIS 2025"),
                    ("seed_doi", "Seed DOI list"),
                ],
                db_index=True,
                max_length=32,
            ),
        ),
    ]