import argparse
import sys
import os
import os.path
import traceback
import logging
from six.moves.urllib import parse as urlparse
from concurrent.futures import ProcessPoolExecutor

from weather.data import DATA_FILE_PATTERN
from weather import is_dir, FullPaths, TYPES, init_ftp, FTP_BASE_DIR, DEFAULT_DOWNLOAD_DIR

logger = logging.getLogger(__name__)

def download(types, output_dir):
    if "all" in types:
        types = TYPES
    if types:
        with ProcessPoolExecutor(max_workers=len(types)) as executor:
            futures = []
            for source_type in types:
                logger.info("downloading {0} sources...".format(source_type))
                source_download_dir = os.path.join(output_dir, source_type)
                if not os.path.exists(source_download_dir):
                    os.makedirs(source_download_dir)
                futures.append(executor.submit(download_type, source_type, source_download_dir))
            saved = []
            for future in futures:
                result = future.result()
                if result:
                    saved.extend(result)
            for path in saved:
                logger.debug("downloaded '%s'", path)
            logger.info("downloaded types %s", types)
    else:
        logger.info("nothing to download")


def download_type(type, output_dir):
    ftp = init_ftp()
    saved = []
    dirs = ("recent", "historical")
    if type == "solar":
        # no splitting into recent and historical here
        dirs = (".", )

    for dir in dirs:
        ftp.cwd(FTP_BASE_DIR)
        ftp.cwd(type + "/" + dir)

        for file_info in ftp.mlsd():
            name = file_info[0]
            if name not in (".", ".."):
                match = DATA_FILE_PATTERN.match(name)
                if match:
                    store_path = os.path.join(output_dir, name)
                    if os.path.exists(store_path):
                        logger.info("already downloaded: '%s'", store_path)
                        continue

                    ftp.retrbinary('RETR %s' % name, open(store_path, 'wb').write)
                    saved.append(store_path)
                else:
                    logger.debug("name '%s' no valid data file", name)
    ftp.quit()
    return saved


def main():
    parser = argparse.ArgumentParser(description="Download data")
    parser.add_argument('types', type=str, action='append', help="""The type of data sources to download.

options are:

 * precipitation
 * sun
 * air_temperature
 * solar
 * soil_temparature
 * cloudiness
 * pressure
 * all (all of the above)
""", choices=TYPES + ["all"])
    parser.add_argument('--download-dir', dest="download_dir", action=FullPaths,
                        type=is_dir, help="the directory to download the data sources into",
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
        download(args.types, args.download_dir)
        sys.exit(0)
    except Exception:
        traceback.print_exc()
        sys.exit(1)

