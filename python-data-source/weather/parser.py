
import sys
import os
import traceback
import logging
import argparse
import gzip
import json
import multiprocessing

from .data import Parsers
from weather import FullPaths, is_dir, DEFAULT_DOWNLOAD_DIR, DEFAULT_OUT_DIR
from concurrent.futures import ProcessPoolExecutor
from .stations import get_stations
from .data.station_metadata import StationsMetadata

logger = logging.getLogger(__name__)


def parse(download_dir, stations=None, out_dir=DEFAULT_OUT_DIR, normalized=False):
    if not stations:
        logger.info("no stations given")
        return

    os.makedirs(out_dir, exist_ok=True)
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = {}
        for station_id in stations:
            futures[station_id] = executor.submit(run_parse_station, download_dir, station_id, out_dir, normalized)

        stations_metadata = StationsMetadata()
        for station_id, future in futures.items():
            try:
                result = future.result()
                stations_metadata.update(result)
            except:
                logger.error("error handling station %s", station_id, exc_info=True)
        if normalized:
            logger.info("writing station metadata...")
            stations_metadata.write_to(out_dir)
    logger.info("done")


def run_parse_station(*args):
    try:
        return parse_station(*args)
    except Exception:
        raise Exception("".join(traceback.format_exception(*sys.exc_info())))


def parse_station(download_dir, station_id, out_dir, normalized):
    outfile = os.path.join(out_dir, station_id + ".json.gz")
    with gzip.open(outfile, "w") as json_file:
        logger.debug("writing to {0}".format(json_file.name))
        parsed = Parsers.parse(download_dir, station_id, normalized)
        for row in parsed["data"]:
            if row:
                json_file.write(json.dumps(row).encode("utf-8"))
                json_file.write(b"\n")
            else:
                continue
    return parsed["metadata"]


def main():
    parser = argparse.ArgumentParser(description="Parse Weather data")
    parser.add_argument("--download-dir", dest="download_dir", action=FullPaths,
                        type=is_dir, help="the directory the data sources were downloaded into",
                        default=DEFAULT_DOWNLOAD_DIR)
    parser.add_argument("--out-dir", dest="out_dir",
                        type=str, help="the output directory",
                        default=DEFAULT_OUT_DIR)
    parser.add_argument("--station", dest="stations", type=str, nargs='*', help="the station to parse")
    parser.add_argument('-d', '--debug', dest="debug", action="store_true")
    parser.add_argument('--normalized', dest="normalized", action="store_true", help="if true, create data for normalized schema")
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
        stations = args.stations
        if not stations:
            stations = get_stations(args.download_dir)

        parse(args.download_dir, stations, args.out_dir, args.normalized)
        sys.exit(0)
    except Exception:
        traceback.print_exc()
        sys.exit(1)
