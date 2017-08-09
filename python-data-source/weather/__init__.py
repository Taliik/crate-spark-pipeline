import argparse
import os
import os.path
from ftplib import FTP

__version__ = '0.0.0'


TYPES = [
    "precipitation",
    "sun",
    "air_temperature",
    "solar",
    "soil_temperature",
    "cloudiness",
    "pressure",
    "wind"
]

CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "etc"))
DEFAULT_DOWNLOAD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "downloads"))
DEFAULT_OUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "out"))

FTP_SERVER = "ftp-cdc.dwd.de"
FTP_BASE_DIR = "/pub/CDC/observations_germany/climate/hourly"


def init_ftp():
    ftp = FTP(FTP_SERVER)
    ftp.login()
    ftp.cwd(FTP_BASE_DIR)
    return ftp


class FullPaths(argparse.Action):
    """Expand user- and relative-paths"""
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, os.path.abspath(os.path.expanduser(values)))


def is_dir(dirname):
    """
    Checks if a path is an actual directory.
    Tries to create the directory.
    """
    if not os.path.isdir(dirname):
        if not os.path.exists(dirname):
            try:
                os.makedirs(dirname)
            except Exception as e:
                raise argparse.ArgumentTypeError(
                    "could not create directory {0}".format(dirname))
        else:
            msg = "{0} exists but is not a directory".format(dirname)
            raise argparse.ArgumentTypeError(msg)
    else:
        return dirname
