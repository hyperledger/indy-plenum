"""
plenum package

"""
from __future__ import absolute_import, division, print_function

import sys

import plenum
from plenum.common.util import check_deps

if sys.version_info < (3, 5, 0):
    raise ImportError("Python 3.5.0 or later required.")


import importlib
from .__metadata__ import *

check_deps(plenum)
