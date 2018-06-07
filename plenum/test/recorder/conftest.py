import math

import pytest


from plenum.common.util import randomString
from plenum.test.pool_transactions.helper import sdk_add_new_nym
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.recorder.helper import create_recorder_for_test, \
    reload_modules_for_recorder


@pytest.fixture(scope="module")
def tconf(tconf):
    tconf.STACK_COMPANION = 1
    reload_modules_for_recorder(tconf)
    return tconf


TOTAL_TXNS = 2


@pytest.fixture(scope="module")
def some_txns_done(tconf, txnPoolNodesLooper, txnPoolNodeSet, sdk_pool_handle,
                   sdk_wallet_steward):
    for i in range(math.ceil(TOTAL_TXNS / 2)):
        sdk_add_new_nym(txnPoolNodesLooper, sdk_pool_handle, sdk_wallet_steward,
                        alias='testSteward' + randomString(100))
    for i in range(math.floor(TOTAL_TXNS / 2)):
        sdk_send_random_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 5)


@pytest.fixture()
def recorder(tmpdir_factory):
    return create_recorder_for_test(tmpdir_factory, 'test_db')
