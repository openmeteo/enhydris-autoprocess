from django.test import TestCase

from model_mommy import mommy

from enhydris.models import Station
from enhydris_autoprocess.admin import CurveInterpolationForm
from enhydris_autoprocess.models import CurveInterpolation, CurvePoint


class CurveInterpolationFormTestCase(TestCase):
    def setUp(self):
        self.station = mommy.make(Station)
        self.ci = mommy.make(
            CurveInterpolation,
            station=self.station,
            source_timeseries__gentity=self.station,
            target_timeseries__gentity=self.station,
            name="Stage-discharge",
        )
        point = CurvePoint(curve_interpolation=self.ci, x=2.718, y=3.141)
        point.save()
        point = CurvePoint(curve_interpolation=self.ci, x=4, y=5)
        point.save()

    def test_init(self):
        form = CurveInterpolationForm(instance=self.ci, station=self.station)
        content = form.as_p()
        self.assertTrue("2.718\t3.141\n4.0\t5.0" in content)

    def test_save(self):
        form = CurveInterpolationForm(
            {
                "source_timeseries": self.ci.source_timeseries.id,
                "target_timeseries": self.ci.target_timeseries.id,
                "points": "1\t2",
            },
            instance=self.ci,
            station=self.station,
        )
        self.assertTrue(form.is_valid())
        form.save()
        point = CurvePoint.objects.get(curve_interpolation=self.ci)
        self.assertAlmostEqual(point.x, 1)
        self.assertAlmostEqual(point.y, 2)

    def test_validate(self):
        form = CurveInterpolationForm(
            {
                "source_timeseries": self.ci.source_timeseries.id,
                "target_timeseries": self.ci.target_timeseries.id,
                "points": "garbage",
            },
            instance=self.ci,
            station=self.station,
        )
        self.assertFalse(form.is_valid())
        self.assertEqual(
            form.errors["points"][0],
            'Error in line 1: "garbage" is not a valid pair of numbers',
        )
