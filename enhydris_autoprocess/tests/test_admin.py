import datetime as dt

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings

from model_mommy import mommy

from enhydris.models import Station
from enhydris_autoprocess.admin import CurvePeriodForm
from enhydris_autoprocess.models import CurveInterpolation, CurvePeriod, CurvePoint

User = get_user_model()


class CurvePeriodFormTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        self.ci = mommy.make(
            CurveInterpolation,
            station=self.station,
            source_timeseries__gentity=self.station,
            target_timeseries__gentity=self.station,
            name="Stage-discharge",
        )
        self.period = mommy.make(
            CurvePeriod,
            curve_interpolation=self.ci,
            start_date=dt.date(1980, 1, 1),
            end_date=dt.date(1985, 6, 30),
        )
        point = CurvePoint(curve_period=self.period, x=2.718, y=3.141)
        point.save()
        point = CurvePoint(curve_period=self.period, x=4, y=5)
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
        point = CurvePoint.objects.get(curve_period=self.period)
        period = CurvePeriod.objects.get(curve_interpolation=self.ci)
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
        self.station = mommy.make(Station, creator=self.user)

    def test_curve_periods_are_shown(self):
        assert self.client.login(username="alice", password="topsecret") is True
        response = self.client.get(
            "/admin/enhydris/station/{}/change/".format(self.station.id)
        )
        self.assertContains(response, "Curve periods")
