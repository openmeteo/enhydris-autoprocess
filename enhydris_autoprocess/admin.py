from django import forms
from django.utils.translation import gettext_lazy as _

import nested_admin

from enhydris.admin.station import InlinePermissionsMixin, StationAdmin
from enhydris.models import Timeseries

from . import models


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
        model = models.RangeCheck
        fields = (
            "source_timeseries",
            "target_timeseries",
            "lower_bound",
            "upper_bound",
        )


class RangeCheckInline(InlinePermissionsMixin, nested_admin.NestedTabularInline):
    model = models.RangeCheck
    classes = ("collapse",)
    formset = AutoProcessFormSet
    form = RangeCheckForm
    verbose_name = _("Range check")
    verbose_name_plural = _("Range checks")


StationAdmin.inlines.append(RangeCheckInline)


class CurveInterpolationForm(AutoProcessForm):
    class Meta:
        model = models.RangeCheck
        fields = ("source_timeseries", "target_timeseries")


class CurvePointInline(nested_admin.NestedTabularInline):
    model = models.CurvePoint


class CurveInterpolationInline(
    InlinePermissionsMixin, nested_admin.NestedTabularInline
):
    model = models.CurveInterpolation
    classes = ("collapse",)
    formset = AutoProcessFormSet
    form = CurveInterpolationForm
    inlines = [CurvePointInline]


StationAdmin.inlines.append(CurveInterpolationInline)
