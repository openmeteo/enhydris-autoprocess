import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("enhydris", "0037_timeseries_groups"),
        ("enhydris_autoprocess", "0008_aggregation_max_missing"),
    ]

    operations = [
        migrations.AlterField(
            model_name="autoprocess",
            name="source_timeseries",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="validation",
                to="enhydris.Timeseries",
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="autoprocess",
            name="target_timeseries",
            field=models.OneToOneField(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="validation",
                to="enhydris.Timeseries",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="autoprocess",
            name="timeseries_group",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="enhydris.TimeseriesGroup",
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="aggregation",
            name="target_time_step",
            field=models.CharField(
                default="FIXME",
                help_text=(
                    'E.g. "10min", "H" (hourly), "D" (daily), "M" (monthly), '
                    '"Y" (yearly). More specifically, it\'s an optional number plus '
                    "a unit, with no space in between. The units available are min, "
                    "H, D, M, Y."
                ),
                max_length=7,
            ),
            preserve_default=False,
        ),
        migrations.CreateModel(
            name="Checks",
            fields=[
                (
                    "autoprocess_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="enhydris_autoprocess.AutoProcess",
                    ),
                ),
            ],
            bases=("enhydris_autoprocess.autoprocess",),
        ),
        migrations.AddField(
            model_name="rangecheck",
            name="checks",
            field=models.OneToOneField(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                serialize=False,
                to="enhydris_autoprocess.Checks",
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="curveinterpolation",
            name="target_timeseries_group",
            field=models.ForeignKey(
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="enhydris.TimeseriesGroup",
            ),
            preserve_default=False,
        ),
    ]
