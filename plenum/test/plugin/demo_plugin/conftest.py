import importlib

import pytest
from copy import deepcopy

from plenum import setup_plugins, PLUGIN_LEDGER_IDS, PLUGIN_CLIENT_REQUEST_FIELDS
from plenum.common.pkg_util import update_module_vars
from plenum.test.plugin.demo_plugin.main import integrate_plugin_in_node


def do_plugin_initialisation_for_tests():
    # The next imports and reloading are needed only in tests, since in
    # production none of these modules would be loaded before plugins are
    # setup (not initialised)
    import plenum.server
    import plenum.common

    importlib.reload(plenum.server.replica)
    importlib.reload(plenum.server.node)
    importlib.reload(plenum.server.view_change.view_changer)
    importlib.reload(plenum.server.message_handlers)
    importlib.reload(plenum.server.observer.observable)
    importlib.reload(plenum.common.ledger_manager)


@pytest.fixture(scope="module")
def tconf(tconf, request):
    global PLUGIN_LEDGER_IDS, PLUGIN_CLIENT_REQUEST_FIELDS

    orig_plugin_root = deepcopy(tconf.PLUGIN_ROOT)
    orig_enabled_plugins = deepcopy(tconf.ENABLED_PLUGINS)
    orig_plugin_ledger_ids = deepcopy(PLUGIN_LEDGER_IDS)
    orig_plugin_client_req_fields = deepcopy(PLUGIN_CLIENT_REQUEST_FIELDS)

    update_module_vars('plenum.config',
                       **{
                           'PLUGIN_ROOT': 'plenum.test.plugin',
                           'ENABLED_PLUGINS': ['demo_plugin', ],
                       })
    PLUGIN_LEDGER_IDS = set()
    PLUGIN_CLIENT_REQUEST_FIELDS = {}
    setup_plugins()
    do_plugin_initialisation_for_tests()

    def reset():
        global PLUGIN_LEDGER_IDS, PLUGIN_CLIENT_REQUEST_FIELDS
        update_module_vars('plenum.config',
                           **{
                               'PLUGIN_ROOT': orig_plugin_root,
                               'ENABLED_PLUGINS': orig_enabled_plugins,
                           })
        PLUGIN_LEDGER_IDS = orig_plugin_ledger_ids
        PLUGIN_CLIENT_REQUEST_FIELDS = orig_plugin_client_req_fields
        setup_plugins()

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope="module")
def do_post_node_creation():
    # Integrate plugin into each node.
    def _post_node_creation(node):
        integrate_plugin_in_node(node)

    return _post_node_creation


@pytest.fixture(scope="module")
def txn_pool_node_set_post_creation(tconf, do_post_node_creation, txnPoolNodeSet):
    return txnPoolNodeSet
