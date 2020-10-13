import datetime as dt

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from bs4 import BeautifulSoup
from model_mommy import mommy

import enhydris.models
from enhydris.tests.admin import get_formset_parameters
from enhydris_autoprocess import models
from enhydris_autoprocess.admin import CurvePeriodForm

User = get_user_model()


class TestCaseBase(TestCase):
    def _create_data(self):
        self.user = User.objects.create_user(
            username="alice",
            password="topsecret",
            is_active=True,
            is_staff=True,
            is_superuser=False,
        )
        self.organization = enhydris.models.Organization.objects.create(
            name="Serial killers SA"
        )
        self.variable = mommy.make(enhydris.models.Variable, descr="myvar")
        self.unit = mommy.make(enhydris.models.UnitOfMeasurement)
        self.time_zone = mommy.make(
            enhydris.models.TimeZone, code="EET", utc_offset=120
        )
        self.station = mommy.make(
            enhydris.models.Station, creator=self.user, owner=self.organization
        )
        self.client.login(username="alice", password="topsecret")

    def _post_form(self, data):
        return self.client.post(
            f"/admin/enhydris/station/{self.station.id}/change/", data
        )

    def _get_form(self):
        return self.client.get(f"/admin/enhydris/station/{self.station.id}/change/")


class TimeseriesGroupFormTestCaseBase(TestCaseBase):
    def _get_basic_form_contents(self):
        return {
            "name": "Hobbiton",
            "copyright_years": "2018",
            "copyright_holder": "Bilbo Baggins",
            "owner": self.organization.id,
            "geom_0": "20.94565",
            "geom_1": "39.12102",
            **get_formset_parameters(
                self.client, f"/admin/enhydris/station/{self.station.id}/change/"
            ),
            "timeseriesgroup_set-TOTAL_FORMS": "1",
            "timeseriesgroup_set-INITIAL_FORMS": "0",
            "timeseriesgroup_set-0-variable": self.variable.id,
            "timeseriesgroup_set-0-unit_of_measurement": self.unit.id,
            "timeseriesgroup_set-0-precision": 2,
            "timeseriesgroup_set-0-time_zone": self.time_zone.id,
            "timeseriesgroup_set-0-timeseries_set-INITIAL_FORMS": "0",
        }

    def _create_range_check(self):
        self.timeseries_group = mommy.make(
            enhydris.models.TimeseriesGroup,
            variable=self.variable,
            gentity=self.station,
        )
        self.range_check = mommy.make(
            models.RangeCheck,
            checks__timeseries_group=self.timeseries_group,
            lower_bound=1,
            soft_lower_bound=2,
            soft_upper_bound=3,
            upper_bound=4,
        )


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class TimeseriesGroupFormRangeCheckValidationTestCase(TimeseriesGroupFormTestCaseBase):
    def setUp(self):
        self._create_data()
        self.data = self._get_basic_form_contents()

    def test_returns_error_if_only_upper_bound_is_specified(self):
        data = {**self.data, "timeseriesgroup_set-0-upper_bound": 420}
        response = self._post_form(data)
        self.assertContains(response, "lower and upper bound must be specified")

    def test_returns_error_if_only_lower_bound_is_specified(self):
        data = {**self.data, "timeseriesgroup_set-0-lower_bound": 42}
        response = self._post_form(data)
        self.assertContains(response, "lower and upper bound must be specified")

    def test_succeeds_if_no_bounds_are_specified(self):
        data = self.data
        response = self._post_form(data)
        self.assertEqual(response.status_code, 302)

    def test_succeeds_if_both_upper_and_lower_bounds_are_specified(self):
        data = {
            **self.data,
            "timeseriesgroup_set-0-upper_bound": 420,
            "timeseriesgroup_set-0-lower_bound": 0,
        }
        response = self._post_form(data)
        self.assertEqual(response.status_code, 302)


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class TimeseriesGroupFormCreatesRangeCheckTestCase(TimeseriesGroupFormTestCaseBase):
    def setUp(self):
        self._create_data()
        self._get_response()
        self.range_check = models.RangeCheck.objects.first()

    def _get_response(self):
        data = {
            **self._get_basic_form_contents(),
            "timeseriesgroup_set-0-lower_bound": 42,
            "timeseriesgroup_set-0-soft_lower_bound": 84,
            "timeseriesgroup_set-0-soft_upper_bound": 168,
            "timeseriesgroup_set-0-upper_bound": 420,
        }
        response = self._post_form(data)
        assert response.status_code == 302

    def test_lower_bound(self):
        self.assertEqual(self.range_check.lower_bound, 42)

    def test_soft_lower_bound(self):
        self.assertEqual(self.range_check.soft_lower_bound, 84)

    def test_soft_upper_bound(self):
        self.assertEqual(self.range_check.soft_upper_bound, 168)

    def test_upper_bound(self):
        self.assertEqual(self.range_check.upper_bound, 420)


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class TimeseriesGroupFormSavesExistingRangeCheckTestCase(
    TimeseriesGroupFormCreatesRangeCheckTestCase
):
    def setUp(self):
        self._create_data()
        self._create_range_check()
        self._get_response()
        range_checks = models.RangeCheck.objects.all()
        assert range_checks.count() == 1
        self.range_check = range_checks[0]

    def _get_response(self):
        data = {
            **self._get_basic_form_contents(),
            "timeseriesgroup_set-0-id": self.timeseries_group.id,
            "timeseriesgroup_set-0-gentity": self.station.id,
            "timeseriesgroup_set-0-lower_bound": 42,
            "timeseriesgroup_set-0-soft_lower_bound": 84,
            "timeseriesgroup_set-0-soft_upper_bound": 168,
            "timeseriesgroup_set-0-upper_bound": 420,
        }
        response = self._post_form(data)
        assert response.status_code == 302


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class TimeseriesGroupFormDeletesRangeCheckTestCase(TimeseriesGroupFormTestCaseBase):
    def setUp(self):
        self._create_data()
        self._create_range_check()
        assert models.RangeCheck.objects.count() == 1
        assert models.Checks.objects.count() == 1
        self._get_response()

    def _get_response(self):
        data = {
            **self._get_basic_form_contents(),
            "timeseriesgroup_set-0-id": self.timeseries_group.id,
            "timeseriesgroup_set-0-gentity": self.station.id,
            "timeseriesgroup_set-0-lower_bound": "",
            "timeseriesgroup_set-0-soft_lower_bound": "",
            "timeseriesgroup_set-0-soft_upper_bound": "",
            "timeseriesgroup_set-0-upper_bound": "",
        }
        response = self._post_form(data)
        assert response.status_code == 302

    def test_range_check_has_been_deleted(self):
        self.assertEqual(models.RangeCheck.objects.count(), 0)

    def test_checks_has_been_deleted(self):
        self.assertEqual(models.Checks.objects.count(), 0)


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class TimeseriesGroupFormRangeCheckInitialValuesTestCase(
    TimeseriesGroupFormTestCaseBase
):
    def setUp(self):
        self._create_data()
        self._create_range_check()
        self._get_response()

    def _get_response(self):
        self.response = self._get_form()
        self.soup = BeautifulSoup(self.response.content, "html.parser")

    def test_lower_bound(self):
        value = self.soup.find(id="id_timeseriesgroup_set-0-lower_bound")["value"]
        self.assertEqual(value, "1.0")

    def test_soft_lower_bound(self):
        value = self.soup.find(id="id_timeseriesgroup_set-0-soft_lower_bound")["value"]
        self.assertEqual(value, "2.0")

    def test_soft_upper_bound(self):
        value = self.soup.find(id="id_timeseriesgroup_set-0-soft_upper_bound")["value"]
        self.assertEqual(value, "3.0")

    def test_upper_bound(self):
        value = self.soup.find(id="id_timeseriesgroup_set-0-upper_bound")["value"]
        self.assertEqual(value, "4.0")


class CurvePeriodFormTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(enhydris.models.Station)
        self.ci = mommy.make(
            models.CurveInterpolation,
            timeseries_group__gentity=self.station,
            target_timeseries_group__gentity=self.station,
        )
        self.period = mommy.make(
            models.CurvePeriod,
            curve_interpolation=self.ci,
            start_date=dt.date(1980, 1, 1),
            end_date=dt.date(1985, 6, 30),
        )
        point = models.CurvePoint(curve_period=self.period, x=2.718, y=3.141)
        point.save()
        point = models.CurvePoint(curve_period=self.period, x=4, y=5)
        point.save()

    def test_init(self):
        form = CurvePeriodForm(instance=self.period)
        content = form.as_p()
        self.assertTrue("2.718\t3.141\n4.0\t5.0" in content)

    def test_save(self):
        form = CurvePeriodForm(
            {
                "start_date": dt.date(2019, 9, 3),
                "end_date": dt.date(2021, 9, 3),
                "points": "1\t2",
            },
            instance=self.period,
        )
        self.assertTrue(form.is_valid())
        form.save()
        point = models.CurvePoint.objects.get(curve_period=self.period)
        period = models.CurvePeriod.objects.get(curve_interpolation=self.ci)
        self.assertEqual(period.start_date, dt.date(2019, 9, 3))
        self.assertEqual(period.end_date, dt.date(2021, 9, 3))
        self.assertAlmostEqual(point.x, 1)
        self.assertAlmostEqual(point.y, 2)

    def test_validate(self):
        form = CurvePeriodForm(
            {
                "start_date": dt.date(2019, 9, 3),
                "end_date": dt.date(2021, 9, 3),
                "points": "garbage",
            },
            instance=self.period,
        )
        self.assertFalse(form.is_valid())
        self.assertEqual(
            form.errors["points"][0],
            'Error in line 1: "garbage" is not a valid pair of numbers',
        )


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class CurvePeriodsPermissionTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            username="alice",
            email="alice@alice.com",
            password="topsecret",
            is_active=True,
            is_staff=True,
            is_superuser=False,
        )
        self.station = mommy.make(enhydris.models.Station, creator=self.user)

    def test_curve_periods_are_shown(self):
        assert self.client.login(username="alice", password="topsecret") is True
        response = self.client.get(
            "/admin/enhydris/station/{}/change/".format(self.station.id)
        )
        self.assertContains(response, "Curve periods")


@override_settings(ENHYDRIS_USERS_CAN_ADD_CONTENT=True)
class CurveInterpolationInlineTargetTimeseriesGroupTestCase(TestCaseBase):
    def setUp(self):
        self._create_data()
        self.station2 = mommy.make(
            enhydris.models.Station, creator=self.user, owner=self.organization
        )
        self.timeseries_group = mommy.make(
            enhydris.models.TimeseriesGroup,
            gentity=self.station,
            variable=self.variable,
        )
        self.timeseries_group2 = mommy.make(
            enhydris.models.TimeseriesGroup,
            gentity=self.station2,
            variable=self.variable,
        )

    def test_target_timeseries_group_dropdown_contains_options_from_station1(self):
        response = self._get_form()
        self.assertContains(
            response,
            f'<option value="{self.timeseries_group.id}">myvar</option>',
            html=True,
        )

    def test_target_timeseries_group_dropdown_not_contains_options_from_station2(self):
        response = self._get_form()
        self.assertNotContains(
            response,
            f'<option value="{self.timeseries_group2.id}">myvar</option>',
            html=True,
        )

    def test_target_timeseries_group_dropdown_is_empty_when_adding_station(self):
        response = self.client.get("/admin/enhydris/station/add/")
        self.assertContains(
            response, '<option value="" selected> --------- </option>', html=True
        )
        self.assertNotContains(
            response,
            f'<option value="{self.timeseries_group.id}">myvar</option>',
            html=True,
        )
        self.assertNotContains(
            response,
            f'<option value="{self.timeseries_group2.id}">myvar</option>',
            html=True,
        )
