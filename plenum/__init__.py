"""
plenum package

"""
from __future__ import absolute_import, division, print_function

import sys
if sys.version_info < (3, 5, 0):
    raise ImportError("Python 3.5.0 or later required.")

import os   # noqa
from importlib.util import module_from_spec, spec_from_file_location    # noqa: E402

import plenum   # noqa: E402
import plenum.server.plugin     # noqa: E402
from plenum.common.config_util import getConfig   # noqa: E402

config = getConfig()


PLUGIN_LEDGER_IDS = set()
PLUGIN_CLIENT_REQUEST_FIELDS = {}


def setup_plugins():
    global PLUGIN_LEDGER_IDS

    ENABLED_PLUGINS = config.ENABLED_PLUGINS
    for plugin_name in ENABLED_PLUGINS:
        plugin_path = os.path.join(plenum.server.plugin.__path__[0],
                                   plugin_name, '__init__.py')
        spec = spec_from_file_location('__init__.py', plugin_path)
        init = module_from_spec(spec)
        spec.loader.exec_module(init)
        plugin_globals = init.__dict__
        if 'LEDGER_IDS' in plugin_globals:
            PLUGIN_LEDGER_IDS.update(plugin_globals['LEDGER_IDS'])
        if 'CLIENT_REQUEST_FIELDS' in plugin_globals:
            PLUGIN_CLIENT_REQUEST_FIELDS.update(plugin_globals['CLIENT_REQUEST_FIELDS'])


setup_plugins()


from .__metadata__ import *  # noqa

from plenum.common.jsonpickle_util import setUpJsonpickle   # noqa: E402
setUpJsonpickle()
