==========================================================
Enhydris-autoprocess - Automatically process incoming data
==========================================================

.. image:: https://travis-ci.org/openmeteo/enhydris-autoprocess.svg?branch=master
    :alt: Build button
    :target: https://travis-ci.org/openmeteo/enhydris-autoprocess

.. image:: https://codecov.io/github/openmeteo/enhydris-autoprocess/coverage.svg?branch=master
    :alt: Coverage
    :target: https://codecov.io/gh/openmeteo/enhydris-autoprocess

Enhydris-autoprocess is an app that automatically processes time series
to produce new time series. For example, it performs range checking,
saving a new time series that is range checked.

Installing and configuring
==========================

- Install Enhydris 3 or later

- Make sure ``enhydris_autoprocess`` is in the PYTHONPATH, or link to it
  from the top-level directory of Enhydris.

- In the Enhydris ``enhydris_project/settings/local.py`` file, add
  ``enhydris_autoprocess`` to ``INSTALLED_APPS``.

- In the Enhydris configuration directory, execute ``python manage.py
  migrate``.

- Run ``celery``.

- Go to the admin, visit a station, and see the "auto-process" section
  at the bottom.

Technical description
=====================

You have a meteorological station called "Hobbiton". It measures
temperature. Because of sensor, transmission, or other errors,
sometimes the temperature is wrong—for example, 280 °C. What you want
to do (and what this app does, among other things) is delete these
measurements automatically as they come in. In this case, assuming
that the low and high all-time temperature records in Hobbiton are -18
and +38 °C, you might decide that anything below -25 or above +50 °C
(the "hard" limits) is an error, whereas anything below -20 or above
+40 °C (the "soft" limits) is a suspect value. In that case, you
configure enhydris-autoprocess with the soft and hard limits. Each
time data is uploaded, an event is triggered, resulting in an
asynchronous process processing the initial uploaded data, deleting the
values outside the hard limits, flagging as suspect the values outside
the soft limits, and saving the result to the "checked" time series of
the time series group.

(More specifically, enhydris-autoprocess uses the ``post_save`` Django
signal for ``enhydris.Timeseries`` to trigger a Celery task that does
the auto processing—see ``apps.py`` and ``tasks.py``.)

Range checking is only one of the ways in which a time series can be
auto-processed—there's also aggregation (e.g. deriving hourly from
ten-minute time series) and curve interpolation (e.g. deriving
discharge from stage, or estimating the air speed at a height of 2 m
above ground when the wind sensor is at a different height). The name
we use for all these together (i.e. checking, aggregation,
interpolation) is "auto process". Technically, ``AutoProcess`` is the
super class and it has some subclasses such as ``Checks``,
``Aggregation`` and ``CurveInterpolation``. These are implemented
using Django's multi-table inheritance. (The checking subclass is
called ``Checks`` because there can be many checks—range checking,
time consistency checking, etc; these are performed one after the
other and they result in the "checked" time series.)

``AutoProcess`` objects have these attributes and methods:

- ``timeseries_group``. The time series group to which this
  auto-process applies.
- ``execute()``. Performs the auto-processing. It retrieves the new
  part of the source time series (i.e. the part that starts after the
  last date of the target time series) and calls the
  ``process_timeseries()`` method.
- ``source_timeseries`` (property). The source time series of the time
  series group for this auto-process. It depends on the kind of
  auto-process: for ``Checks`` it is the initial time series; for
  ``Aggregation`` and ``CurveInterpolation`` it is the checked time
  series if it exists, or the initial if it does not exist. If no
  suitable time series exists, it is created.
- ``target_timeseries`` (property). The target time series of the time
  series group for this auto-process. It depends on the kind of
  auto-process: for ``Checks`` it is the checked time series; for
  ``Aggregation`` it is the aggregated time series with the
  target time step; for ``CurveInterpolation`` it is the initial
  time series of the target time series group (``CurveInterpolation``
  has an additional ``target_timeseries_group`` attribute). The target
  time series is created if it does not exist.
- ``process_timeseries()``. Performs the actual processing.

Meta
====

© 2019-2020 National Technical University of Athens
© 2019-2020 Institute of Communication and Computer Systems

Enhydris-autoprocess was funded by NTUA_ and ICCS_ as part of the
OpenHi_ project.

Enhydris-autoprocess is free software, available under the GNU Affero
General Public License.

.. _ntua: https://www.ntua.gr/
.. _iccs: https://www.iccs.gr/
.. _openhi: https://openhi.net/
