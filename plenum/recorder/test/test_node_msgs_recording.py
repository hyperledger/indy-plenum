import time

import pytest

from plenum.common.constants import STEWARD_STRING
from plenum.common.util import randomString
from plenum.test.helper import sdk_send_random_and_check
from plenum.test.pool_transactions.helper import sdk_add_new_nym


@pytest.fixture(scope="module")
def some_txns_done(txnPoolNodesLooper, txnPoolNodeSet, sdk_pool_handle,
                   sdk_wallet_steward):
    for i in range(4):
        sdk_add_new_nym(txnPoolNodesLooper, sdk_pool_handle, sdk_wallet_steward,
                        alias='testSteward' + randomString(3),
                        role=STEWARD_STRING)
    for i in range(3):
        sdk_send_random_and_check(txnPoolNodesLooper, txnPoolNodeSet,
                                  sdk_pool_handle, sdk_wallet_steward, 5)


def test_record_node_msgs(some_txns_done, txnPoolNodeSet):
    recorders = []
    for node in txnPoolNodeSet:
        assert node.nodestack.recorder.store.size > 0
        assert node.clientstack.recorder.store.size > 0
        node.nodestack.recorder.start_playing()
        node.clientstack.recorder.start_playing()
        recorders.append(node.nodestack.recorder)
        recorders.append(node.clientstack.recorder)

    # Drain each recorder
    while any([recorder.is_playing for recorder in recorders]):
        for recorder in recorders:
            recorder.get_next()
        time.sleep(0.01)
