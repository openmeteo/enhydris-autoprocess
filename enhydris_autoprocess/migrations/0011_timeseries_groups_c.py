import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("enhydris", "0037_timeseries_groups"),
        ("enhydris_autoprocess", "0010_timeseries_groups_b"),
    ]

    operations = [
        migrations.AlterField(
            model_name="autoprocess",
            name="timeseries_group",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                to="enhydris.TimeseriesGroup",
            ),
            preserve_default=False,
        ),
        migrations.RemoveField(model_name="rangecheck", name="autoprocess_ptr"),
        migrations.AlterField(
            model_name="rangecheck",
            name="checks",
            field=models.OneToOneField(
                default=0,
                on_delete=django.db.models.deletion.CASCADE,
                primary_key=True,
                serialize=False,
                to="enhydris_autoprocess.Checks",
            ),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name="curveinterpolation",
            name="target_timeseries_group",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                to="enhydris.TimeseriesGroup",
            ),
            preserve_default=False,
        ),
        migrations.RemoveField(model_name="autoprocess", name="source_timeseries"),
        migrations.RemoveField(model_name="autoprocess", name="target_timeseries"),
        migrations.RemoveField(model_name="autoprocess", name="station"),
        migrations.RemoveField(model_name="curveinterpolation", name="name"),
    ]
