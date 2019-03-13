"""
plenum package metadata
"""
import os
import json
from typing import Tuple, List, Union
import collections.abc

from common.version import PlenumVersion, InvalidVersionError

VERSION_FILENAME = '__version__.json'
VERSION_FILE = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), VERSION_FILENAME)


def load_version(version_file: str = VERSION_FILE) -> PlenumVersion:
    with open(version_file, 'r') as _f:
        version = json.load(_f)
        if not isinstance(version, collections.abc.Iterable):
            raise InvalidVersionError(
                "Failed to load from {}: data '{}' is not iterable"
                .format(version_file, version)
            )
        return PlenumVersion('.'.join([str(i) for i in version if str(i)]))


def set_version(version: str, version_file: str = VERSION_FILE):
    version = PlenumVersion(version)
    with open(version_file, 'w') as _f:
        json.dump(['' if i is None else i for i in version.parts], _f)
        _f.write('\n')


__title__ = 'indy-plenum'
__version_info__ = load_version()
__version__ = __version_info__.full
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
    '__license__',
    'load_version',
    'set_version'
]
