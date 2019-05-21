import datetime as dt

from django.db import models

import numpy as np
import pandas as pd

from enhydris.models import Timeseries


class Validation(models.Model):
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
