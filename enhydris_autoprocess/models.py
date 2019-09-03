import csv
import datetime as dt
from io import StringIO

from django.db import IntegrityError, models
from django.utils.translation import gettext_lazy as _

import numpy as np
import pandas as pd

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
        htimeseries = self.source_timeseries.get_data(start_date=self._get_start_date())
        result = self.process_timeseries(htimeseries)
        self.target_timeseries.append_data(result)

    def process_timeseries(self, htimeseries):
        for alternative in ("rangecheck", "curveinterpolation"):
            if hasattr(self, alternative):
                return getattr(self, alternative).process_timeseries(htimeseries)

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

    def __str__(self):
        return _("Range check for {}").format(str(self.source_timeseries))

    def process_timeseries(self, ahtimeseries):
        timeseries = ahtimeseries.data
        out_of_bounds_mask = ~pd.isnull(timeseries["value"]) & ~timeseries[
            "value"
        ].between(self.lower_bound, self.upper_bound)
        timeseries.loc[out_of_bounds_mask, "value"] = np.nan

        out_of_bounds_with_no_flags_mask = out_of_bounds_mask & (
            timeseries["flags"] == ""
        )
        out_of_bounds_with_flags_mask = (
            out_of_bounds_mask & ~out_of_bounds_with_no_flags_mask
        )
        timeseries.loc[out_of_bounds_with_no_flags_mask, "flags"] = "RANGE"
        timeseries.loc[out_of_bounds_with_flags_mask, "flags"] = (
            timeseries.loc[out_of_bounds_with_flags_mask, "flags"] + " RANGE"
        )
        return timeseries


class CurveInterpolation(AutoProcess):
    name = models.CharField(max_length=200)

    def __str__(self):
        return self.name

    def process_timeseries(self, ahtimeseries):
        timeseries = ahtimeseries.data
        for period in self.curveperiod_set.order_by("start_date"):
            x, y = period._get_curve()
            start, end = period.start_date, period.end_date
            values_array = timeseries.loc[start:end, "value"].values
            new_array = np.interp(values_array, x, y)
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
