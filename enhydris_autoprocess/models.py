import datetime as dt

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

    def __str__(self):
        return str(self.source_timeseries)

    def execute(self):
        timeseries = self.source_timeseries.get_data(start_date=self._get_start_date())
        for check in ["rangecheck"]:
            if hasattr(self, check):
                getattr(self, check).execute(timeseries)
        self.target_timeseries.append_data(timeseries)

    def _get_start_date(self):
        start_date = self.target_timeseries.end_date
        if start_date:
            start_date += dt.timedelta(minutes=1)
        return start_date

    def save(self, *args, **kwargs):
        self._check_integrity()
        result = super().save(*args, **kwargs)
        tasks.execute_auto_process.delay(self.id)
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


class RangeCheck(models.Model):
    auto_process = models.OneToOneField(AutoProcess, on_delete=models.CASCADE)
    upper_bound = models.FloatField()
    lower_bound = models.FloatField()

    def __str__(self):
        return str(self.auto_process)

    def execute(self, ahtimeseries):
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
