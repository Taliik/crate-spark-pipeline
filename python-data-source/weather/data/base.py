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

from zipfile import ZipFile, BadZipfile
import datetime
from pytz import utc as UTC
import os
import glob
import csv
import logging
logger = logging.getLogger(__name__)


EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)


class DWDDataSourceParser(object):
    """
    base class for parsing DWD sources
    """
    RECENT = "recent"
    HISTORICAL = "historical"

    def __init__(self, download_dir, work_dir, stations_metadata, normalized=False):
        self.download_dir = download_dir
        self.stations_metadata = stations_metadata
        self.workdir = os.path.join(work_dir, self.get_name())
        self.include_metadata = not normalized

    def get_metadata(self, station_id, infile):
        station_metadata = None
        if station_id not in self.stations_metadata:

            with open(infile, 'r', encoding='iso8859') as md_file:
                reader = csv.DictReader(md_file,
                                        delimiter=';',
                                        fieldnames=["id", "height", "lon", "lat", "from_date", "to_date", "name"])
                next(reader)  # skip header
                for row in reader:
                    if not row:
                        continue
                    id = row["id"].strip()
                    name = row["name"].strip()
                    from_date = self.get_date(row["from_date"].strip(), with_hour=False)
                    to_date = self.get_date(row["to_date"].strip(), with_hour=False)
                    lon = self.get_float(row["lon"].strip())
                    lat = self.get_float(row["lat"].strip())
                    if None in (lon, lat):
                        position = None
                    else:
                        position = [
                            lon,
                            lat
                        ]

                    metadata = {
                        "station_id": id,
                        "station_height": self.get_float(row["height"].strip()),
                        "position_lon": position[0],
                        "position_lat": position[1],
                        "station_name": name
                    }
                    station_metadata = self.stations_metadata.get_or_create(station_id)
                    station_metadata.name = name
                    station_metadata.add(from_date, to_date, metadata)
        else:
            station_metadata = self.stations_metadata[station_id]
        return station_metadata

    def parse(self, station_id):
        glob_matches = glob.glob(os.path.join(
            self.download_dir,
            self.get_name(),
            "stundenwerte_*_{}*.zip".format(station_id))
        )
        if not glob_matches:
            logger.debug("could not find file for station %s in dir '%s'" % (station_id, os.path.join(self.download_dir, self.get_name())))
            return []

        metadata_file, data_files = self.open_zip(glob_matches)
        try:
            metadata = self.get_metadata(station_id, metadata_file)
        finally:
            os.unlink(metadata_file)
        return self.parse_data(data_files, metadata)

    def open_zip(self, infiles):
        metadata_extracted = False
        metadata = None
        data_files = []
        for infile in infiles:
            try:
                zip = ZipFile(infile, 'r')
                for efile in zip.namelist():
                    if not metadata_extracted and efile.startswith('Metadaten'):

                        zip.extract(efile, path=self.workdir)
                        metadata = os.path.join(self.workdir, efile)
                        metadata_extracted = True
                        logger.debug("metadata: %s", metadata)
                    if efile.startswith('produkt'):

                        zip.extract(efile, path=self.workdir)
                        data_files.append(
                            os.path.join(self.workdir, efile)
                        )
                        logger.debug("data: %s", os.path.join(self.workdir, efile))
            except BadZipfile as e:
                logger.error("could not open: {}".format(infile))
                raise e
        return metadata, data_files

    @staticmethod
    def get_date(val, with_hour=True):
        if not val:
            return None
        hour_value = 0
        if with_hour:
            hour_value = int(val[8:10], 10)
        # TODO: check if UTC is right for every datum
        dt = datetime.datetime(year=int(val[0:4], 10), month=int(val[4:6], 10), day=int(val[6:8], 10), hour=hour_value, tzinfo=UTC)
        return int((dt - EPOCH).total_seconds() * 1000)


    @staticmethod
    def get_float(val):
        try:
            fval = float(val)
        except ValueError as e:
            logger.debug("error converting '%s' to float", val)
            return None
        if fval == -999:
            return None
        return fval

    @staticmethod
    def get_int(val):
        try:
            ival = int(val)
        except ValueError as e:
            logger.debug("error converting '%s' to int", val)
            return None
        if ival == -999:
            return None
        return ival

    @classmethod
    def get_name(cls):
        return "base"

    def parse_data(self, infiles, metadata):
        for infile in infiles:
            try:
                with open(infile, 'r') as data:
                    reader = csv.reader(data, delimiter=';')
                    i = 0
                    header_skipped = False
                    for row in reader:
                        if not header_skipped:
                            header_skipped = True
                            continue
                        i += 1
                        try:
                            if len(row) >= self.expected_columns() and filter(None, row):
                                date = self.get_date(row[1])
                                data = self.extract_data(row)
                                data["date"] = date
                                data["station_id"] = metadata.id
                                if self.include_metadata:
                                    date_metadata = metadata.get_by_date(date)
                                    if not date_metadata:
                                        date_metadata = metadata.any()
                                    data.update(date_metadata)
                                yield data
                        except Exception as e:
                            logger.error("Error during {}, row {}: {}".format(self.get_name(), i, row))
                            logger.error(e)
                            yield {}
            finally:
                os.unlink(infile)

    def extract_data(self, row):
        raise NotImplementedError()

    def expected_columns(self):
        return 1000
