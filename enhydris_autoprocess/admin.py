from io import StringIO

from django import forms
from django.db import models
from django.utils.translation import gettext_lazy as _

import nested_admin

from enhydris.admin.station import (
    InlinePermissionsMixin,
    StationAdmin,
    TimeseriesGroupInline,
)
from enhydris.models import TimeseriesGroup

from .models import Aggregation, Checks, CurveInterpolation, CurvePeriod, RangeCheck

# We override StationAdmin's render_change_form method in order to specify a custom
# template. We do this in order to offer some model-wide help (help_text is only
# available for fields).


def render_change_form(self, *args, **kwargs):
    self.change_form_template = "enhydris_autoprocess/station_change_form.html"
    return super(StationAdmin, self).render_change_form(*args, **kwargs)


StationAdmin.render_change_form = render_change_form


class TimeseriesGroupForm(forms.ModelForm):
    lower_bound = forms.FloatField(required=False)
    soft_lower_bound = forms.FloatField(required=False)
    soft_upper_bound = forms.FloatField(required=False)
    upper_bound = forms.FloatField(required=False)

    class Meta:
        model = TimeseriesGroup
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._populate_range_check_fields()

    def _populate_range_check_fields(self):
        if not getattr(self, "instance", None):
            return
        try:
            range_check = RangeCheck.objects.get(checks__timeseries_group=self.instance)
            self.fields["lower_bound"].initial = range_check.lower_bound
            self.fields["soft_lower_bound"].initial = range_check.soft_lower_bound
            self.fields["soft_upper_bound"].initial = range_check.soft_upper_bound
            self.fields["upper_bound"].initial = range_check.upper_bound
        except RangeCheck.DoesNotExist:
            pass

    def clean(self):
        self._check_that_bounds_are_present_or_absent()
        return super().clean()

    def _check_that_bounds_are_present_or_absent(self):
        hard_bounds = [
            self.cleaned_data[x] is not None for x in ("lower_bound", "upper_bound")
        ]
        soft_bounds = [
            self.cleaned_data[f"soft_{x}_bound"] is not None for x in ("lower", "upper")
        ]
        if all(hard_bounds) or (not any(hard_bounds) and not any(soft_bounds)):
            return
        raise forms.ValidationError(
            _(
                "To perform a range check, lower and upper bound must be specified; "
                "otherwise, all four bounds must be empty."
            )
        )

    def save(self, *args, **kwargs):
        result = super().save(*args, **kwargs)
        self._save_range_check()
        return result

    def _save_range_check(self):
        if self.cleaned_data["lower_bound"] is None:
            self._delete_range_check()
        else:
            self._create_or_update_range_check()

    def _delete_range_check(self):
        try:
            checks = Checks.objects.get(timeseries_group=self.instance)
            range_check = RangeCheck.objects.get(checks=checks)
            range_check.delete()
            # IMPORTANT: When a check besides range check is implemented, the following
            # line must be refined.
            checks.delete()
        except (Checks.DoesNotExist, RangeCheck.DoesNotExist):
            pass

    def _create_or_update_range_check(self):
        checks, created = Checks.objects.get_or_create(timeseries_group=self.instance)
        try:
            self._save_existing_range_check(checks)
        except RangeCheck.DoesNotExist:
            self._save_new_range_check(checks)

    def _save_existing_range_check(self, checks):
        range_check = RangeCheck.objects.get(checks=checks)
        range_check.lower_bound = self.cleaned_data["lower_bound"]
        range_check.soft_lower_bound = self.cleaned_data["soft_lower_bound"]
        range_check.soft_upper_bound = self.cleaned_data["soft_upper_bound"]
        range_check.upper_bound = self.cleaned_data["upper_bound"]
        range_check.save()

    def _save_new_range_check(self, checks):
        RangeCheck.objects.create(
            checks=checks,
            lower_bound=self.cleaned_data["lower_bound"],
            soft_lower_bound=self.cleaned_data["soft_lower_bound"],
            soft_upper_bound=self.cleaned_data["soft_upper_bound"],
            upper_bound=self.cleaned_data["upper_bound"],
        )


TimeseriesGroupInline.form = TimeseriesGroupForm
TimeseriesGroupInline.fieldsets.append(
    (
        _("Range check"),
        {
            "fields": (
                ("lower_bound", "soft_lower_bound", "soft_upper_bound", "upper_bound"),
            ),
            "classes": ("collapse",),
        },
    ),
)


class CurvePeriodForm(forms.ModelForm):
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
        model = CurvePeriod
        fields = ("start_date", "end_date", "points")

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


class CurvePeriodInline(InlinePermissionsMixin, nested_admin.NestedTabularInline):
    model = CurvePeriod
    form = CurvePeriodForm
    points = models.CharField()
    extra = 1


class CurveInterpolationForm(forms.ModelForm):
    class Meta:
        model = CurveInterpolation
        fields = "__all__"


class CurveInterpolationInline(
    InlinePermissionsMixin, nested_admin.NestedTabularInline
):
    model = CurveInterpolation
    fk_name = "timeseries_group"
    classes = ("collapse",)
    form = CurveInterpolationForm
    inlines = [CurvePeriodInline]
    extra = 1

    def formfield_for_foreignkey(self, db_field, request, **kwargs):
        if db_field.name == "target_timeseries_group":
            try:
                station_id = request.path.strip("/").split("/")[-2]
                kwargs["queryset"] = TimeseriesGroup.objects.filter(gentity=station_id)
            except ValueError:
                kwargs["queryset"] = TimeseriesGroup.objects.none()
        return super().formfield_for_foreignkey(db_field, request, **kwargs)


TimeseriesGroupInline.inlines.append(CurveInterpolationInline)


class AggregationForm(forms.ModelForm):
    class Meta:
        model = Aggregation
        fields = (
            "target_time_step",
            "method",
            "max_missing",
            "resulting_timestamp_offset",
        )
        widgets = {"resulting_timestamp_offset": forms.TextInput(attrs={"size": 7})}


class AggregationInline(InlinePermissionsMixin, nested_admin.NestedTabularInline):
    model = Aggregation
    classes = ("collapse",)
    form = AggregationForm
    verbose_name = _("Aggregation")
    verbose_name_plural = _("Aggregations")
    extra = 1


TimeseriesGroupInline.inlines.append(AggregationInline)
