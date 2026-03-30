from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("tracing", "0003_narrative_multi"),
    ]

    operations = [
        migrations.AddField(
            model_name="trace",
            name="annotation_status",
            field=models.CharField(
                choices=[
                    ("unannotated", "Unannotated"),
                    ("annotated", "Annotated"),
                    ("invalid", "Invalid — figure type unconfirmed"),
                ],
                db_index=True,
                default="unannotated",
                max_length=12,
            ),
        ),
    ]