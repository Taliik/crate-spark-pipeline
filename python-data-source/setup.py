import os
import re

from setuptools import setup, find_packages

versionf_content = open("weather/__init__.py").read()
version_rex = r'^__version__ = [\'"]([^\'"]*)[\'"]$'
m = re.search(version_rex, versionf_content, re.M)
if m:
    VERSION = m.group(1)
else:
    raise RuntimeError('Unable to find version string')


def get_versions():
    """picks the versions from version.cfg and returns them as dict"""
    from six.moves import configparser
    versions_cfg = os.path.join(os.path.dirname(__file__), 'versions.cfg')
    config = configparser.ConfigParser()
    config.optionxform = str
    config.readfp(open(versions_cfg))
    return dict(config.items('versions'))


def nailed_requires(requirements, pat=re.compile(r'^(.+)(\[.+\])?$')):
    """returns the requirements list with nailed versions"""
    versions = get_versions()
    res = []
    for req in requirements:
        if '[' in req:
            name = req.split('[', 1)[0]
        else:
            name = req
        if name in versions:
            res.append('%s==%s' % (req, versions[name]))
        else:
            res.append(req)
    return res

requires = [
    'six',
    'urllib3',
    'pytz'
]

setup(name='weather',
      version=VERSION,
      author='Crate GmbH',
      author_email='hello@crate.io',
      packages=find_packages(),
      include_package_data=True,
      extras_require=dict(
      ),
      zip_safe=False,
      install_requires=requires,
      entry_points={
         'console_scripts': [
             'parse_data = weather.parser:main',
             'download = weather.download:main',
             'stations = weather.stations:main'
         ]
      },
)
