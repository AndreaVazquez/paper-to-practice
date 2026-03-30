# Generated manually — adds 'observablehq' to RepoSource.platform choices.
# SQLite does not enforce CharField choices at the DB level, so this migration
# carries no SQL; it exists only to keep the Django migration state coherent.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("repository", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="reposource",
            name="platform",
            field=models.CharField(
                choices=[
                    ("kaggle", "Kaggle"),
                    ("github", "GitHub"),
                    ("observablehq", "ObservableHQ"),
                ],
                db_index=True,
                max_length=16,
            ),
        ),
    ]