import csv
import datetime as dt
import re
from io import StringIO

from django.db import IntegrityError, models
from django.utils.translation import gettext_lazy as _

import numpy as np
import pandas as pd
from haggregate import aggregate, regularize

from enhydris.models import Station, Timeseries

from . import tasks


class AutoProcess(models.Model):
    station = models.ForeignKey(Station, on_delete=models.CASCADE)
    source_timeseries = models.OneToOneField(
        Timeseries, on_delete=models.CASCADE, related_name="auto_process"
    )
    target_timeseries = models.OneToOneField(
        Timeseries, on_delete=models.CASCADE, related_name="target_timeseries_of"
    )

    class Meta:
        verbose_name_plural = _("Auto processes")

    def execute(self):
        self.htimeseries = self.source_timeseries.get_data(
            start_date=self._get_start_date()
        )
        result = self.process_timeseries()
        self.target_timeseries.append_data(result)

    def process_timeseries(self):
        for alternative in ("rangecheck", "curveinterpolation", "aggregation"):
            if hasattr(self, alternative):
                childobj = getattr(self, alternative)
                childobj.htimeseries = self.htimeseries
                return childobj.process_timeseries()

    def _get_start_date(self):
        start_date = self.target_timeseries.end_date
        if start_date:
            start_date += dt.timedelta(minutes=1)
        return start_date

    def save(self, *args, **kwargs):
        self._check_integrity()
        result = super().save(*args, **kwargs)

        # We delay running the task by one second; otherwise it may run before the
        # current transaction has been committed.
        tasks.execute_auto_process.apply_async(args=[self.id], countdown=1)

        return result

    def _check_integrity(self):
        if self.source_timeseries.gentity.id != self.station.id:
            raise IntegrityError(
                "AutoProcess.source_timeseries must belong to AutoProcess.station"
            )
        if self.target_timeseries.gentity.id != self.station.id:
            raise IntegrityError(
                "AutoProcess.target_timeseries must belong to AutoProcess.station"
            )


class RangeCheck(AutoProcess):
    upper_bound = models.FloatField()
    lower_bound = models.FloatField()
    soft_upper_bound = models.FloatField(blank=True, null=True)
    soft_lower_bound = models.FloatField(blank=True, null=True)

    def __str__(self):
        return _("Range check for {}").format(str(self.source_timeseries))

    def process_timeseries(self):
        self._do_hard_limits()
        self._do_soft_limits()
        return self.htimeseries.data

    def _do_hard_limits(self):
        self._find_out_of_bounds_values(self.lower_bound, self.upper_bound)
        self._replace_out_of_bounds_values_with_nan()
        self._add_flag_to_out_of_bounds_values("RANGE")
        return self.htimeseries.data

    def _do_soft_limits(self):
        self._find_out_of_bounds_values(self.soft_lower_bound, self.soft_upper_bound)
        self._add_flag_to_out_of_bounds_values("SUSPECT")

    def _find_out_of_bounds_values(self, low, high):
        timeseries = self.htimeseries.data
        self.out_of_bounds_mask = ~pd.isnull(timeseries["value"]) & ~timeseries[
            "value"
        ].between(low, high)

    def _replace_out_of_bounds_values_with_nan(self):
        self.htimeseries.data.loc[self.out_of_bounds_mask, "value"] = np.nan

    def _add_flag_to_out_of_bounds_values(self, flag):
        d = self.htimeseries.data
        out_of_bounds_with_flags_mask = self.out_of_bounds_mask & (d["flags"] != "")
        d.loc[out_of_bounds_with_flags_mask, "flags"] += " "
        d.loc[self.out_of_bounds_mask, "flags"] += flag
        return d


class CurveInterpolation(AutoProcess):
    name = models.CharField(max_length=200)

    def __str__(self):
        return self.name

    def process_timeseries(self):
        timeseries = self.htimeseries.data
        for period in self.curveperiod_set.order_by("start_date"):
            x, y = period._get_curve()
            start, end = period.start_date, period.end_date
            values_array = timeseries.loc[start:end, "value"].values
            new_array = np.interp(values_array, x, y, left=np.nan, right=np.nan)
            timeseries.loc[start:end, "value"] = new_array
            timeseries.loc[start:end, "flags"] = ""
        return timeseries


class CurvePeriod(models.Model):
    curve_interpolation = models.ForeignKey(
        CurveInterpolation, on_delete=models.CASCADE
    )
    start_date = models.DateField()
    end_date = models.DateField()

    def __str__(self):
        return "{}: {} - {}".format(
            str(self.curve_interpolation), self.start_date, self.end_date
        )

    def _get_curve(self):
        x = []
        y = []
        for point in self.curvepoint_set.filter(curve_period=self).order_by("x"):
            x.append(point.x)
            y.append(point.y)
        return x, y

    def set_curve(self, s):
        """Replaces all existing points with ones read from a string.

        The string can be comma-delimited or tab-delimited, or a mix.
        """

        s = s.replace("\t", ",")
        self.curvepoint_set.all().delete()
        for row in csv.reader(StringIO(s)):
            x, y = [float(item) for item in row[:2]]
            CurvePoint.objects.create(curve_period=self, x=x, y=y)


class CurvePoint(models.Model):
    curve_period = models.ForeignKey(CurvePeriod, on_delete=models.CASCADE)
    x = models.FloatField()
    y = models.FloatField()

    def __str__(self):
        return _("{}: Point ({}, {})").format(str(self.curve_period), self.x, self.y)


class Aggregation(AutoProcess):
    METHOD_CHOICES = [
        ("sum", "Sum"),
        ("mean", "Mean"),
        ("max", "Max"),
        ("min", "Min"),
    ]
    method = models.CharField(max_length=4, choices=METHOD_CHOICES)
    resulting_timestamp_offset = models.CharField(
        max_length=7,
        blank=True,
        help_text=(
            'If the time step of the target time series is one day ("D") and you set '
            'the resulting timestamp offset to "1min", the resulting time stamps will '
            "be ending in 23:59.  This does not modify the calculations; it only "
            "subtracts the specified offset from the timestamp after the calculations "
            "have finished. Leave empty to leave the timestamps alone."
        ),
    )

    def __str__(self):
        return _("Aggregation for {}").format(str(self.source_timeseries))

    def save(self, force_insert=False, force_update=False, *args, **kwargs):
        self._check_resulting_timestamp_offset()
        super().save(force_insert, force_update, *args, **kwargs)

    def _check_resulting_timestamp_offset(self):
        if not self.resulting_timestamp_offset:
            return
        else:
            self._check_nonempty_resulting_timestamp_offset()

    def _check_nonempty_resulting_timestamp_offset(self):
        m = re.match(r"(-?)(\d*)(.*)$", self.resulting_timestamp_offset)
        sign, number, unit = m.group(1, 2, 3)
        if unit != "min" or (sign == "-" and number == ""):
            raise IntegrityError(
                '"{}" is not a valid resulting time step offset.'.format(
                    self.resulting_timestamp_offset
                )
            )

    def process_timeseries(self):
        self._regularize_time_series()
        self._aggregate_time_series()
        return self.htimeseries

    def _regularize_time_series(self):
        self.htimeseries = regularize(self.htimeseries, new_date_flag="DATEINSERT")

    def _aggregate_time_series(self):
        source_step = self._get_source_step()
        target_step = self._get_target_step()
        min_count = self._divide_target_step_by_source_step(source_step, target_step)
        self.htimeseries = aggregate(
            self.htimeseries,
            target_step,
            self.method,
            min_count=min_count,
            target_timestamp_offset=self.resulting_timestamp_offset or None,
        )

    def _get_source_step(self):
        return pd.infer_freq(self.htimeseries.data.index)

    def _get_target_step(self):
        result = self.target_timeseries.time_step
        if not result[0].isdigit():
            result = "1" + result
        return result

    def _divide_target_step_by_source_step(self, source_step, target_step):
        return int(
            pd.Timedelta(target_step) / pd.tseries.frequencies.to_offset(source_step)
        )
