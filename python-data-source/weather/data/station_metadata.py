# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, if you have executed another commercial license agreement
# with Crate these terms will supersede the license and you may use the
# software solely pursuant to the terms of the relevant commercial agreement.
from collections import UserDict
import json
import gzip
import os
import logging
logger = logging.getLogger(__name__)


class StationsMetadata(UserDict):

    STATION_FILENAME = "stations.json.gz"
    STATION_LOCATIONS_FILENAME = "station_locations.json.gz"

    def get_metadata(self, station_id, date):
        station_metadata = self.get(station_id, None)
        if station_metadata:
            return station_metadata.get_metadata(date)
        return None

    def get_or_create(self, station_id):
        if station_id not in self:
            md = StationMetadata(station_id)
            self[station_id] = md
            return md
        else:
            return self[station_id]

    def write_to(self, out_dir):
        station_file_path = os.path.abspath(os.path.join(out_dir, self.STATION_FILENAME))
        station_locations_file_path = os.path.abspath(os.path.join(out_dir, self.STATION_LOCATIONS_FILENAME))
        with gzip.open(station_file_path, "w") as station_file:
            with gzip.open(station_locations_file_path, "w") as station_locations_file:
                for station_id, station_metadata in self.items():
                    station_row = {
                        "id": station_id,
                        "name": station_metadata.name
                    }
                    station_file.write(json.dumps(station_row).encode("utf-8"))
                    station_file.write(b"\n")
                    for dates, md in station_metadata.items():
                        station_location_row = {
                            "station_id": station_id,
                            "from_date": dates.from_date,
                            "to_date": dates.to_date,
                            "position": md["position"],
                            "height": md["station_height"]
                        }
                        station_locations_file.write(json.dumps(station_location_row).encode("utf-8"))
                        station_locations_file.write(b"\n")
        logger.info("station metadata successfully written to %s", out_dir)


class StationMetadata(UserDict):

    def __init__(self, id):
        super().__init__({})
        self.id = id
        self._name = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self._name is None or value != self._name:
            self._name = value

    def add(self, from_date, to_date, metadata):
        dates = Dates(from_date, to_date)
        self[dates] = metadata

    def get_by_date(self, date):
        for dates, md in self.items():
            if date in dates:
                return md
        return None

    def any(self):
        if self:
            return next(iter(self.values()))
        else:
            return None


class Dates(object):

    def __init__(self, from_date, to_date):
        self.from_date = from_date
        self.to_date = to_date

    def __contains__(self, date):
        gte = self.from_date is None or self.from_date <= date
        lte = self.to_date is None or date <= self.to_date
        return gte and lte

    def __eq__(self, other):
        return self.from_date == other.from_date and self.to_date == other.to_date

    def __hash__(self):
        return hash(self.from_date) ^ hash(self.to_date)

    def __str__(self):
        return "%s - %s" % (self.from_date, self.to_date)
