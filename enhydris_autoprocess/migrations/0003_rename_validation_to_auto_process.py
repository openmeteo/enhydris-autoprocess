import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("enhydris_autoprocess", "0002_validation_station")]

    operations = [
        migrations.RenameModel("Validation", "AutoProcess"),
        migrations.AlterModelOptions(
            name="autoprocess", options={"verbose_name_plural": "Auto processes"}
        ),
        migrations.RenameField(
            model_name="RangeCheck", old_name="validation", new_name="auto_process"
        ),
        migrations.AlterField(
            model_name="autoprocess",
            name="source_timeseries",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="auto_process",
                to="enhydris.Timeseries",
            ),
        ),
    ]
