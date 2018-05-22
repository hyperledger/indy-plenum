import math

import pytest

from plenum.common.util import randomString
from plenum.recorder.src.recorder import Recorder
from plenum.recorder.test.helper import reload_modules_for_recorder
from plenum.test.conftest import _tconf
from plenum.test.helper import sdk_send_random_and_check
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys

from plenum.test.conftest import *  # noqa

# overriddenConfigValues['USE_WITH_STACK'] = 1
#
#
# @pytest.fixture(scope="module")
# def tconf(general_conf_tdir):
#     conf = _tconf(general_conf_tdir)
#     reload_modules_for_recorder(conf)
#     return conf


@pytest.fixture()
def recorder(tmpdir_factory):
    storage = KeyValueStorageLeveldbIntKeys(
        tmpdir_factory.mktemp('').strpath, 'test_db')
    return Recorder(storage)


TOTAL_TXNS = 2


@pytest.fixture(scope="module")
def some_txns_done(txnPoolNodesLooper, txnPoolNodeSet, sdk_pool_handle,
                   sdk_wallet_steward):
    for i in range(math.ceil(TOTAL_TXNS/2)):
        sdk_add_new_nym(txnPoolNodesLooper, sdk_pool_handle, sdk_wallet_steward,
                        alias='testSteward' + randomString(100))
    for i in range(math.floor(TOTAL_TXNS/2)):
        sdk_send_random_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 5)
