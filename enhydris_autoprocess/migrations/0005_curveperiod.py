import datetime as dt

import django.db.models.deletion
from django.db import migrations, models


def fix_data(apps, schema_editor):
    CurvePeriod = apps.get_model("enhydris_autoprocess", "CurvePeriod")
    CurveInterpolation = apps.get_model("enhydris_autoprocess", "CurveInterpolation")
    CurvePoint = apps.get_model("enhydris_autoprocess", "CurvePoint")
    for interpolation in CurveInterpolation.objects.all():
        period = CurvePeriod.objects.create(
            curve_interpolation=interpolation,
            start_date=dt.date(1800, 1, 1),
            end_date=dt.date(2100, 1, 1),
        )
        for point in CurvePoint.objects.filter(curve_interpolation=interpolation):
            point.curve_period = period
            point.save()


def do_nothing(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [("enhydris_autoprocess", "0004_curveinterpolation")]

    operations = [
        migrations.CreateModel(
            name="CurvePeriod",
            fields=[
                (
                    "id",
                    models.AutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("start_date", models.DateField()),
                ("end_date", models.DateField()),
                (
                    "curve_interpolation",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="enhydris_autoprocess.CurveInterpolation",
                    ),
                ),
            ],
        ),
        migrations.AddField(
            model_name="curvepoint",
            name="curve_period",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                to="enhydris_autoprocess.CurvePeriod",
            ),
            preserve_default=False,
        ),
        migrations.RunPython(fix_data, do_nothing),
        migrations.AlterField(
            model_name="curvepoint",
            name="curve_period",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                to="enhydris_autoprocess.CurvePeriod",
            ),
        ),
        migrations.RemoveField(model_name="curvepoint", name="curve_interpolation"),
    ]
