"""
plenum package

"""
from __future__ import absolute_import, division, print_function

import sys
import plenum
from plenum.common.jsonpickle_util import setUpJsonpickle

from .__metadata__ import *  # noqa

if sys.version_info < (3, 5, 0):
    raise ImportError("Python 3.5.0 or later required.")

setUpJsonpickle()
