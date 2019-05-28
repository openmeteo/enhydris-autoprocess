import datetime as dt

from django.db import IntegrityError, models

import numpy as np
import pandas as pd

from enhydris.models import Station, Timeseries


class Validation(models.Model):
    station = models.ForeignKey(Station, on_delete=models.CASCADE)
    source_timeseries = models.OneToOneField(
        Timeseries, on_delete=models.CASCADE, related_name="validation"
    )
    target_timeseries = models.OneToOneField(
        Timeseries, on_delete=models.CASCADE, related_name="target_timeseries_of"
    )

    def __str__(self):
        return str(self.source_timeseries)

    def perform(self):
        timeseries = self.source_timeseries.get_data(start_date=self._get_start_date())
        for check in ["rangecheck"]:
            if hasattr(self, check):
                getattr(self, check).perform(timeseries)
        self.target_timeseries.append_data(timeseries)

    def _get_start_date(self):
        start_date = self.target_timeseries.end_date
        if start_date:
            start_date += dt.timedelta(minutes=1)
        return start_date

    def save(self, *args, **kwargs):
        self._check_integrity()
        return super().save(*args, **kwargs)

    def _check_integrity(self):
        if self.source_timeseries.gentity.id != self.station.id:
            raise IntegrityError(
                "Validation.source_timeseries must belong to Validation.station"
            )
        if self.target_timeseries.gentity.id != self.station.id:
            raise IntegrityError(
                "Validation.target_timeseries must belong to Validation.station"
            )


class RangeCheck(models.Model):
    validation = models.OneToOneField(Validation, on_delete=models.CASCADE)
    upper_bound = models.FloatField()
    lower_bound = models.FloatField()

    def __str__(self):
        return str(self.validation)

    def perform(self, timeseries):
        out_of_bounds_mask = ~pd.isnull(timeseries["value"]) & ~timeseries[
            "value"
        ].between(self.lower_bound, self.upper_bound)
        for timestamp in timeseries[out_of_bounds_mask].index:
            timeseries.loc[timestamp, "value"] = np.nan
            timeseries.loc[timestamp, "flags"] = " ".join(
                timeseries.loc[timestamp, "flags"].split() + ["RANGE"]
            )
