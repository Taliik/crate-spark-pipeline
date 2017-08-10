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

import argparse
import os
import sys
import traceback
import logging

from weather.data import DATA_FILE_PATTERN
from weather import is_dir, FullPaths, DEFAULT_DOWNLOAD_DIR
from weather import TYPES

logger = logging.getLogger(__name__)


def get_stations(download_dir):
    stations = set()
    for type in TYPES:
        for file_path in os.listdir(os.path.join(download_dir, type)):
            match = DATA_FILE_PATTERN.match(file_path)
            if match:
                station_id = match.group(1)
                stations.add(station_id)
    return stations


def main():
    parser = argparse.ArgumentParser(description="Parse Weather data")
    parser.add_argument("--download-dir", dest="download_dir", action=FullPaths,
                        type=is_dir,
                        help="the directory the data sources were downloaded into",
                        default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument('-d', '--debug', dest="debug", action="store_true")
    args = parser.parse_args(sys.argv[1:])
    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(
            format='[%(levelname)s %(asctime)s] %(message)s',
            datefmt='%Y/%m/%d %H:%M:%S',
            stream=sys.stdout,
            level=level)
    try:
        logger.info("found stations:")
        for station in get_stations(args.download_dir):
            print(station)
        sys.exit(0)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
