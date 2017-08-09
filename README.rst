============
Weather Data
============

Download hourly weather measurements from all over germany from the
DWD (Deutscher Wetterdienst) and convert it to gzipped json files suitable
for importing into your `crate`_ cluster.

Requirements
============

 * ``python3``

Initial Setup for Development
=============================

Its suggested to use a virtualenv for development.

Bootstrap with python::

    $ /path/to/python bootstrap.py
    $ bin/buildout -N

Download Source Data from DWD
=============================

Use the ``bin/download`` command to download the sources from the DWD ftp server
to a folder of your choice.

To download all files to the folder ``/tmp/weather``:

    $ bin/download --download-dir /tmp/weather all

It is also possible to download seperate kinds of data only. Instead of ``all``
use one or more of these categories:

    * precipitation
    * sun
    * air_temperature
    * solar
    * soil_temperature
    * cloudiness
    * pressure
    * wind


Convert Data to Gzipped Json
============================

To convert the downloaded files, use the ``bin/parse_data`` command.
It will convert the downloaded data which contains a csv file for each station for each
measurement category into one file with all available data for one station.

The data can be converted into a normalized and denormalized dataset.
Both create one file per station which contains all its measurement data.

Converting normalized dataset will create two special files:

 * ``stations.json.gz`` containing the station id and names
 * ``station_locations.json.gz`` containing the location and height of the station
   for a specific period, which might have changed over time.

Usage
-----

To convert all available data in ``/tmp/weather`` into ``json.gz`` files in the ``/tmp/out`` folder
containing a denormalized dataset::

    $ bin/parse_data --download-dir /tmp/weather --out-dir /tmp/out

For normalized output, use the ``--normalized`` flag.

It is also possible to only convert data for single stations by giving their
station ids (which can be obtained from the names of the downloaded files)::

    $ bin/parse_data --download-dir /tmp/weather --out-dir /tmp/out --station 00003 00044

.. note::

    When converting only single stations for a normalized dataset, the ``stations``
    and ``station_locations`` files will contain only data for the converted stations.
    Old data will be overridden.

Data
====

We got 236 mio. rows (235603810), each containing one or more measurement at one station
at one point in time. The amount of data might increase once the DWD will add new data.

Measurements were taken at 1455 different stations, which might have changed locations
over time, so we have 3946 different locations for those stations in different time periods.

The earliest measurements have been taken at ``Thu Jan 01 03:00:00 UTC 1891``
in Marburg-Cappel, Kiel-Kronshagen and Kassel-Harleshausen.

Schemas
=======

Denormalized
------------

See ``german_climate_denormalized.sql``.

.. code-block:: sql

    CREATE TABLE german_climate_denormalized (
      date timestamp,
      station_id string,
      station_name string,
      position geo_point, -- position of the weather station
      station_height int, -- height of the weather station
      temp float, -- temperature in °C
      humility double, -- relative humility in percent
      cloudiness int,  -- 0 (cloudless)
                       -- 1 or less (nearly cloudless)
                       -- 2 (less cloudy)
                       -- 3
                       -- 4 (cloudy)
                       -- 5
                       -- 6 (more cloudy)
                       -- 7 or more (nearly overcast)
                       -- 8 (overcast)
                       -- -1 not available
      rainfall_fallen boolean, -- if some precipitation happened this hour
      rainfall_height double,  -- precipitation height in mm
      rainfall_form int, -- 0 - no precipiation
                         -- 1 - only "distinct" (german: "abgesetzte") precipitation
                         -- 2 - only liquid "distinct" precipitation (e.g. dew)
                         -- 3 - only solid "distinct" precipitation (e.g. frost)
                         -- 6 - liquid
                         -- 7 - solid
                         -- 8 - solid and liquid
                         -- 9 - no measurement
      air_pressure double,  -- air pressure (Pa)
      air_pressure_station_height double, -- air pressure at station height (Pa)
      ground_temp array(float), -- soil temperature in °C at 2cm, 5cm, 10cm, 20cm and 50cm depth
      sunshine_duration double, -- sum of sunshine duration in that hour in minutes
      diffuse_sky_radiation double, -- sum of diffuse short-wave sky-radiation in J/cm² for that hour
      global_radiation double, -- sum of global short-wave radiation in J/cm² for that hour
      sun_zenith float, -- solar zenith angle (https://en.wikipedia.org/wiki/Solar_zenith_angle) in degree
      wind_speed double, -- wind speed in m/sec
      wind_direction int -- wind direction given in 36-part land-spout
    ) clustered by (station_id) with (number_of_replicas=0, refresh_interval=0);


Normalized
----------

This example schema uses a custom schema name.

See ``german_climate_normalized.sql``.

.. code-block:: sql

    -- a weather station
    CREATE TABLE german_climate.stations (
      id string primary key,
      name string
    ) with (number_of_replicas=0, refresh_interval=0); -- settings for import purposes only

    -- the location of a weather station which might have changed over time
    CREATE TABLE german_climate.station_locations (
      station_id string,
      position geo_point,
      height int, -- height in m
      from_date timestamp, -- station has been at this location from this point in time (inclusive)
      to_date timestamp    -- station has been at this location up to that point in time (inclusive)
    ) clustered by (station_id)
    with (number_of_replicas=0, refresh_interval=0); -- settings for import purposes only


    -- the actual measurement
    -- might not contain data for every possible column
    CREATE TABLE german_climate.data (
      date timestamp primary key,
      station_id string primary key,
      temp float, -- temperature in °C
      humility double, -- relative humility in percent
      cloudiness int,  -- 0 (cloudless)
                       -- 1 or less (nearly cloudless)
                       -- 2 (less cloudy)
                       -- 3
                       -- 4 (cloudy)
                       -- 5
                       -- 6 (more cloudy)
                       -- 7 or more (nearly overcast)
                       -- 8 (overcast)
                       -- -1 not available
      rainfall_fallen boolean, -- if some precipitation happened this hour
      rainfall_height double,  -- precipitation height in mm
      rainfall_form int, -- 0 - no precipitation
                         -- 1 - only "distinct" (german: "abgesetzte") precipitation
                         -- 2 - only liquid "distinct" precipitation (e.g. dew)
                         -- 3 - only solid "distinct" precipitation (e.g. frost)
                         -- 6 - liquid
                         -- 7 - solid
                         -- 8 - solid and liquid
                         -- 9 - no measurement
      air_pressure double,  -- air pressure (Pa)
      air_pressure_station_height double, -- air pressure at station height (Pa)
      ground_temp array(float), -- soil temperature in °C at 2cm, 5cm, 10cm, 20cm and 50cm depth
      sunshine_duration double, -- sum of sunshine duration in that hour in minutes
      diffuse_sky_radiation double, -- sum of diffuse short-wave sky-radiation in J/cm² for that hour
      global_radiation double, -- sum of global short-wave radiation in J/cm² for that hour
      sun_zenith float, -- solar zenith angle (https://en.wikipedia.org/wiki/Solar_zenith_angle) in degree
      wind_speed double, -- wind speed in m/sec
      wind_direction int -- wind direction given in 36-part land-spout
    ) clustered by (station_id) with (number_of_replicas=0, refresh_interval=0);


.. _crate: https://crate.io
