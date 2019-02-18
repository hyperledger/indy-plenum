"""
plenum package metadata
"""
import os
import json

import plenum

VERSION_FILENAME = '__version__.json'
VERSION_FILE = os.path.join(
    os.path.abspath(os.path.dirname(plenum.__file__)), VERSION_FILENAME)


def check_version(version):
    # TODO better errors (e.g. some are TypeError)
    if not (
        (type(version) in (tuple, list)) and
        (len(version) == 5) and
        all([type(version[i]) == int] for i in (0, 1, 2, 4)) and
        (version[3] in ('dev', 'rc', 'stable'))
    ):
        raise ValueError("Incorrect version: {}".format(version))


def load_version(version_file=VERSION_FILE):
    with open(version_file, 'r') as _f:
        version = json.load(_f)
    check_version(version)
    return version


def set_version(version, version_file=VERSION_FILE):
    check_version(version)
    with open(version_file, 'w') as _f:
        version = json.dump(version, _f)
        _f.write('\n')


def pep440_version(version=None):
    if not version:
        version = __version_info__

    check_version(version)
    major, minor, patch, pre_release_suffix, revision = version

    release_part = "{}.{}.{}".format(major, minor, patch)

    if pre_release_suffix == 'stable':
        return release_part
    else:
        return "{}.{}{}".format(release_part, pre_release_suffix, revision)


__title__ = 'indy-plenum'
__version_info__ = load_version()
__version__ = pep440_version()
__author__ = "Hyperledger"
__author_email__ = 'hyperledger-indy@lists.hyperledger.org'
__maintainer__ = 'Hyperledger'
__maintainer_email__ = 'hyperledger-indy@lists.hyperledger.org'
__url__ = 'https://github.com/hyperledger/indy-plenum'
__description__ = 'Plenum Byzantine Fault Tolerant Protocol'
__long_description__ = 'Plenum Byzantine Fault Tolerant Protocol'
__download_url__ = "https://github.com/hyperledger/indy-plenum/tarball/{}".format(__version__)
__license__ = "Apache 2.0"

__all__ = [
    '__title__',
    '__version_info__',
    '__version__',
    '__author__',
    '__author_email__',
    '__maintainer__',
    '__maintainer_email__',
    '__url__',
    '__description__',
    '__long_description__',
    '__download_url__',
    '__license__'
]
