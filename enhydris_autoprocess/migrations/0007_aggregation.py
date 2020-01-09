import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [("enhydris_autoprocess", "0006_soft_limits")]

    operations = [
        migrations.CreateModel(
            name="Aggregation",
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
                (
                    "method",
                    models.CharField(
                        choices=[
                            ("sum", "Sum"),
                            ("mean", "Mean"),
                            ("max", "Max"),
                            ("min", "Min"),
                        ],
                        max_length=4,
                    ),
                ),
                (
                    "resulting_timestamp_offset",
                    models.CharField(
                        blank=True,
                        help_text=(
                            "If the time step of the target time series is one day "
                            '("D") and you set the resulting timestamp offset to '
                            '"1min", the resulting time stamps will be ending in '
                            "23:59.  This does not modify the calculations; it only "
                            "subtracts the specified offset from the timestamp after "
                            "the calculations have finished. Leave empty to leave the "
                            "timestamps alone."
                        ),
                        max_length=7,
                    ),
                ),
            ],
            bases=("enhydris_autoprocess.autoprocess",),
        ),
    ]
