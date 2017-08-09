# -*- coding: utf-8; -*-
#
# Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
# license agreements.  See the NOTICE file distributed with this work for
# additional information regarding copyright ownership.  Crate licenses
# this file to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
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

from .air_temperature import AirTemperatureParser
from .cloudiness import CloudinessParser
from .sun import SunshineParser
from .precipitation import PrecipitationParser
from .wind import WindParser
from .soil_temperature import SoilTemperatureParser
from .pressure import PressureParser
from .solar import SolarRadiationParser
from .station_metadata import StationsMetadata
import re
import os

DATA_FILE_PATTERN = re.compile(r'^stundenwerte_[A-Z0-9]+_(.{5}).*?\.zip$')


def station_file_pattern(station_id):
    return re.compile(r'^stundenwerte_[A-Z0-9]+_(' + station_id + ').*?\.zip$')


class Parsers(object):
    parsers = [
        AirTemperatureParser,
        CloudinessParser,
        PrecipitationParser,
        PressureParser,
        SoilTemperatureParser,
        SolarRadiationParser,
        SunshineParser,
        WindParser,
    ]

    @classmethod
    def parse(cls, download_dir, station_id, normalized):
        data = {}
        metadata = StationsMetadata()
        work_dir = os.path.join(os.path.dirname(download_dir), "var")

        for parser_class in cls.parsers:
            parser = parser_class(download_dir, work_dir, metadata, normalized=normalized)
            for datum in parser.parse(station_id):
                if datum:
                    value = data.setdefault(datum["date"], {})
                    value.update(datum)

        return {
            "data": data.values(),
            "metadata": metadata
        }
