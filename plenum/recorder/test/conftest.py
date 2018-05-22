import math

import pytest

from plenum.common.util import randomString
from plenum.recorder.src.recorder import Recorder
from plenum.recorder.test.helper import reload_modules_for_recorder
from plenum.test.conftest import _tconf
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from plenum.test.pool_transactions.helper import sdk_add_new_nym
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.conftest import *  # noqa
from plenum.test.view_change.conftest import viewNo  # noqa
from plenum.test.node_catchup.conftest import whitelist, sdk_new_node_caught_up, \
    sdk_node_set_with_node_added_after_some_txns, sdk_node_created_after_some_txns


# overriddenConfigValues['USE_WITH_STACK'] = 1
#
#
# @pytest.fixture(scope="module")
# def tconf(general_conf_tdir):
#     conf = _tconf(general_conf_tdir)
#     reload_modules_for_recorder(conf)
#     return conf

TOTAL_TXNS = 2


@pytest.fixture(scope="module")
def some_txns_done(txnPoolNodesLooper, txnPoolNodeSet, sdk_pool_handle,
                   sdk_wallet_steward):
    for i in range(math.ceil(TOTAL_TXNS / 2)):
        sdk_add_new_nym(txnPoolNodesLooper, sdk_pool_handle, sdk_wallet_steward,
                        alias='testSteward' + randomString(100))
    for i in range(math.floor(TOTAL_TXNS / 2)):
        sdk_send_random_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 5)


@pytest.fixture()
def recorder(tmpdir_factory):
    storage = KeyValueStorageLeveldbIntKeys(
        tmpdir_factory.mktemp('').strpath, 'test_db')
    return Recorder(storage)
