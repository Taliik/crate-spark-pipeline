============
Weather Data
============

The directory `python-data-source` provides all the essential software
needed to download and parse weather data in a proper format.
By following the steps hourly weather data (recent or historical) can
be downloaded, parsed and ingested into crate afterwards.

Requirements
============

 * ``python3``
 * ``cr8`` (a simple tool for ingest data remotely)

Initial Setup
=============

Its suggested to use a virtualenv for development.

To install cr8::

    $ pip3 install cr8

Bootstrap with python3::

    $ cd python-data-source
    $ /path/to/python3 bootstrap.py
    $ bin/buildout -N

Download Source Data from DWD
=============================

Use the ``bin/download`` command to download the sources from the DWD ftp
server to a folder of your choice. There are two sources: `recent` and
`historical`.

To download recent weather data to the folder ``/tmp/weather``::

    $ bin/download --download-dir /tmp/weather recent

To download historical weather data to the folder ``/tmp/weather``::

    $ bin/download --download-dir /tmp/weather historical

Convert Data to Gzipped Json
============================

To convert the downloaded files, use the ``bin/parse_data`` command.
It will convert the downloaded data which contains a csv file for each station
for each measurement category into one file with all available data for one
station. The data can be converted into a denormalized dataset.

Usage
-----

To convert all available data in ``/tmp/weather`` into ``json.gz`` files in the
``/tmp/out`` folder containing a denormalized dataset::

    $ bin/parse_data --download-dir /tmp/weather --out-dir /tmp/out

Unpack Data from Parsed Gzipped Json
====================================

To unpack the parsed data located in ``/tmp/out``::

    $ gunzip /tmp/out/*.json.gz

Ingest .json Data into remote or local CrateDB cluster
======================================================

To ingest the .json files located at ``/tmp/out/`` into a table
``weather.germany_raw`` of a remote ``hello.crate.io:4200``(or local) CrateDB
cluster::

    $ cat /tmp/out/*.json | cr8 insert-json --table weather.germany_raw --hosts hello.crate.io:4200

Schema
======

Denormalized
------------

See ``german_climate_denormalized.sql``.

.. code-block:: sql

    CREATE TABLE weather.germany_raw (
        date timestamp primary key,
        station_id string primary key,
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
        ground_temp_2cm float, -- soil temperature in °C at 2cm depth
        ground_temp_5cm float, -- soil temperature in °C at 5cm depth
        ground_temp_10cm float, -- soil temperature in °C at 10cm depth
        ground_temp_20cm float, -- soil temperature in °C at 20cm depth
        ground_temp_50cm float, -- soil temperature in °C at 50cm depth
        ground_temp_100cm float, -- soil temperature in °C at 100cm depth
        sunshine_duration double, -- sum of sunshine duration in that hour in minutes
        wind_speed double, -- wind speed in m/sec
        wind_direction int -- wind direction given in 36-part land-spout
    ) clustered by (station_id) into 16 shards with (number_of_replicas=1, refresh_interval=1000);

.. _crate: https://crate.io
