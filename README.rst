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

© 2019 National Technical University of Athens

Enhydris-autoprocess is free software, available under the GNU Affero
General Public License.

**Installing and configuring**

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
