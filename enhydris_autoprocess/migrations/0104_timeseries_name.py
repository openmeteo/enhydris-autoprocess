import sys

from django.db import migrations


def do_migration(apps, schema_editor):
    Aggregation = apps.get_model("enhydris_autoprocess", "Aggregation")
    Timeseries = apps.get_model("enhydris", "Timeseries")
    for aggregation in Aggregation.objects.all():
        try:
            timeseries = aggregation.timeseries_group.timeseries_set.get(
                type=400, time_step=aggregation.target_time_step, name=""
            )
            timeseries.name = aggregation.get_method_display()
            timeseries.save()
        except Timeseries.DoesNotExist:
            sys.stderr.write(
                "Warning: No target time series found for aggregation with "
                f"id={aggregation.id}\n"
            )


def do_nothing(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("enhydris_autoprocess", "0103_verbose_names"),
        ("enhydris", "0116_timeseries_name"),
    ]

    operations = [
        migrations.RunPython(do_migration, do_nothing),
    ]
