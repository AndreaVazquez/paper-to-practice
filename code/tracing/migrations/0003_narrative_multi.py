"""
Remove the unique constraint from narratives.vis_type so that multiple
narratives can exist for the same visualization type.

SQLite cannot DROP UNIQUE in-place; Django handles this by recreating the
table. The narratives table has 0 rows in production at the time of this
migration so there is no data to preserve.
"""
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("tracing", "0002_rebuild_narrative"),
    ]

    operations = [
        migrations.AlterField(
            model_name="narrative",
            name="vis_type",
            field=models.CharField(db_index=True, max_length=64),
        ),
    ]