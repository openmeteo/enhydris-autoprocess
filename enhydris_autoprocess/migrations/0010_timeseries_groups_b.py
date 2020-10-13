from django.db import migrations


def populate_autoprocess_timeseries_group(apps, schema_editor):
    AutoProcess = apps.get_model("enhydris_autoprocess", "AutoProcess")
    for auto_process in AutoProcess.objects.all():
        auto_process.timeseries_group = auto_process.source_timeseries.timeseries_group
        auto_process.save()


def populate_aggregation_target_time_step(apps, schema_editor):
    Aggregation = apps.get_model("enhydris_autoprocess", "Aggregation")
    for aggregation in Aggregation.objects.all():
        aggregation.target_time_step = aggregation.target_timeseries.time_step
        aggregation.save()


def populate_checks(apps, schema_editor):
    Checks = apps.get_model("enhydris_autoprocess", "Checks")
    RangeCheck = apps.get_model("enhydris_autoprocess", "RangeCheck")
    AutoProcess = apps.get_model("enhydris_autoprocess", "AutoProcess")
    for range_check in RangeCheck.objects.all():
        autoprocess = AutoProcess.objects.get(id=range_check.pk)
        checks = Checks.objects.create(
            station=autoprocess.station,
            timeseries_group=autoprocess.source_timeseries.timeseries_group,
        )
        range_check.checks = checks
        range_check.save()


def populate_curveinterpolation_target_timeseries_group(apps, schema_editor):
    CurveInterpolation = apps.get_model("enhydris_autoprocess", "CurveInterpolation")
    AutoProcess = apps.get_model("enhydris_autoprocess", "AutoProcess")
    for curve_interpolation in CurveInterpolation.objects.all():
        autoprocess = AutoProcess.objects.get(id=curve_interpolation.pk)
        curve_interpolation.target_timeseries_group = (
            autoprocess.target_timeseries.timeseries_group
        )
        curve_interpolation.save()


def do_nothing(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("enhydris", "0037_timeseries_groups"),
        ("enhydris_autoprocess", "0009_timeseries_groups"),
    ]

    operations = [
        migrations.RunPython(populate_autoprocess_timeseries_group, do_nothing),
        migrations.RunPython(populate_aggregation_target_time_step, do_nothing),
        migrations.RunPython(populate_checks, do_nothing),
        migrations.RunPython(
            populate_curveinterpolation_target_timeseries_group, do_nothing
        ),
    ]
