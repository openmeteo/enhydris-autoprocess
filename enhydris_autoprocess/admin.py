from django import forms
from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from enhydris.admin.station import InlinePermissionsMixin, StationAdmin
from enhydris.models import Timeseries

from . import models


class ValidationFormSet(forms.BaseInlineFormSet):
    """Formset that passes station to the form.

    For an explanation of why we need this formset, see
    https://stackoverflow.com/questions/9422735/
    """

    def get_form_kwargs(self, index):
        kwargs = super().get_form_kwargs(index)
        kwargs["station"] = self.instance
        return kwargs


class ValidationForm(forms.ModelForm):
    upper_bound = forms.FloatField(required=True, label=_("Upper bound"))
    lower_bound = forms.FloatField(required=True, label=_("Lower bound"))

    def __init__(self, *args, station, **kwargs):
        super().__init__(*args, **kwargs)
        self.station = station
        t = Timeseries.objects
        self.fields["source_timeseries"].queryset = t.filter(gentity=self.station)
        self.fields["target_timeseries"].queryset = t.filter(gentity=self.station)
        instance = kwargs.get("instance")
        if instance is not None and hasattr(instance, "rangecheck"):
            self.fields["upper_bound"].initial = instance.rangecheck.upper_bound
            self.fields["lower_bound"].initial = instance.rangecheck.lower_bound

    class Meta:
        model = models.Validation
        fields = (
            "source_timeseries",
            "target_timeseries",
            "lower_bound",
            "upper_bound",
        )

    def save(self, instance=None, commit=True):
        instance = super().save(commit=commit)
        if hasattr(instance, "rangecheck"):
            rangecheck = instance.rangecheck
        else:
            rangecheck = models.RangeCheck(validation=instance)
        rangecheck.upper_bound = self.cleaned_data["upper_bound"]
        rangecheck.lower_bound = self.cleaned_data["lower_bound"]
        if commit:
            rangecheck.save()
        return instance


class ValidationInline(InlinePermissionsMixin, admin.TabularInline):
    model = models.Validation
    classes = ("collapse",)
    formset = ValidationFormSet
    form = ValidationForm


StationAdmin.inlines.append(ValidationInline)
