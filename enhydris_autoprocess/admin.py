from io import StringIO

from django import forms
from django.db import models
from django.utils.translation import gettext_lazy as _

import nested_admin

from enhydris.admin.station import InlinePermissionsMixin, StationAdmin
from enhydris.models import Timeseries

from .models import CurveInterpolation, RangeCheck


class AutoProcessFormSet(forms.BaseInlineFormSet):
    """Formset that passes station to the form.

    For an explanation of why we need this formset, see
    https://stackoverflow.com/questions/9422735/
    """

    def get_form_kwargs(self, index):
        kwargs = super().get_form_kwargs(index)
        kwargs["station"] = self.instance
        return kwargs


class AutoProcessForm(forms.ModelForm):
    def __init__(self, *args, station, **kwargs):
        super().__init__(*args, **kwargs)
        self.station = station
        t = Timeseries.objects
        self.fields["source_timeseries"].queryset = t.filter(gentity=self.station)
        self.fields["target_timeseries"].queryset = t.filter(gentity=self.station)


class RangeCheckForm(AutoProcessForm):
    class Meta:
        model = RangeCheck
        fields = (
            "source_timeseries",
            "target_timeseries",
            "lower_bound",
            "upper_bound",
        )


class RangeCheckInline(InlinePermissionsMixin, nested_admin.NestedTabularInline):
    model = RangeCheck
    classes = ("collapse",)
    formset = AutoProcessFormSet
    form = RangeCheckForm
    verbose_name = _("Range check")
    verbose_name_plural = _("Range checks")


StationAdmin.inlines.append(RangeCheckInline)


class CurveInterpolationForm(AutoProcessForm):
    points = forms.CharField(
        widget=forms.Textarea,
        help_text=(
            "The points that form the curve. You can copy/paste them from a "
            "spreadsheet, two columns: X and Y. Copy and paste the points only, "
            "without headings. If you key them in instead, they must be one point "
            "per line, first X then Y, separated by tab or comma."
        ),
    )

    class Meta:
        model = RangeCheck
        fields = ("source_timeseries", "target_timeseries")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            point_queryset = self.instance.curvepoint_set.order_by("x")
            lines = ["{}\t{}".format(p.x, p.y) for p in point_queryset]
            self.initial["points"] = "\n".join(lines)

    def clean_points(self):
        data = self.cleaned_data["points"]
        for i, row in enumerate(StringIO(data)):
            row = row.replace("\t", ",")
            try:
                x, y = [float(item) for item in row.split(",")]
            except ValueError:
                raise forms.ValidationError(
                    'Error in line {}: "{}" is not a valid pair of numbers'.format(
                        i + 1, row
                    )
                )
        return data

    def save(self, *args, **kwargs):
        result = super().save(*args, **kwargs)
        self.instance.set_curve(self.cleaned_data["points"])
        return result


class CurveInterpolationInline(
    InlinePermissionsMixin, nested_admin.NestedTabularInline
):
    model = CurveInterpolation
    classes = ("collapse",)
    formset = AutoProcessFormSet
    form = CurveInterpolationForm
    points = models.CharField()


StationAdmin.inlines.append(CurveInterpolationInline)
