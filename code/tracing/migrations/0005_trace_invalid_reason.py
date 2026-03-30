from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("tracing", "0004_trace_annotation_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="trace",
            name="invalid_reason",
            field=models.TextField(blank=True, default=""),
        ),
    ]