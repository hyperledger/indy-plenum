import pytest

from plenum import setup_plugins, PLUGIN_LEDGER_IDS, PLUGIN_CLIENT_REQUEST_FIELDS
from plenum.test.plugin.demo_plugin.main import update_node_obj
from plenum.test.pool_transactions.conftest import clientAndWallet1, \
    client1, wallet1, client1Connected, looper


@pytest.fixture(scope="module")
def tconf(tconf, request):
    global PLUGIN_LEDGER_IDS, PLUGIN_CLIENT_REQUEST_FIELDS

    orig_plugin_root = tconf.PLUGIN_ROOT
    orig_enabled_plugins = tconf.ENABLED_PLUGINS
    orig_plugin_ledger_ids = PLUGIN_LEDGER_IDS
    orig_plugin_client_req_fields = PLUGIN_CLIENT_REQUEST_FIELDS

    tconf.PLUGIN_ROOT = 'plenum.test.plugin'
    tconf.ENABLED_PLUGINS = ['demo_plugin']
    PLUGIN_LEDGER_IDS = set()
    PLUGIN_CLIENT_REQUEST_FIELDS = {}
    setup_plugins()

    def reset():
        global PLUGIN_LEDGER_IDS, PLUGIN_CLIENT_REQUEST_FIELDS
        tconf.PLUGIN_ROOT = orig_plugin_root
        tconf.ENABLED_PLUGINS = orig_enabled_plugins
        PLUGIN_LEDGER_IDS = orig_plugin_ledger_ids
        PLUGIN_CLIENT_REQUEST_FIELDS = orig_plugin_client_req_fields
        setup_plugins()

    request.addfinalizer(reset)
    return tconf


@pytest.fixture(scope="module")
def txnPoolNodeSet(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        update_node_obj(node)
    return txnPoolNodeSet
