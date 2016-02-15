"""
zeno package

"""
from __future__ import absolute_import, division, print_function

import sys
if sys.version_info < (3, 5, 0):
    raise ImportError("Python 3.5.0 or later required.")


import importlib

#__all__ = ['core']

#_modules = ['core']

#for m in _modules:
#    importlib.import_module(".{0}".format(m), package='zeno')

from .__metadata__ import *
