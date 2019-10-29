from logging import getLogger

import pytest

from plenum.test import waits
from plenum.test.pool_transactions.helper import disconnect_node_and_ensure_disconnected
from plenum.test.helper import max_3pc_batch_limits, sdk_send_random_and_check
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from plenum.test.view_change_with_delays.helper import check_last_ordered
from stp_core.loop.eventually import eventually
from plenum.test.checkpoints.conftest import chkFreqPatched

logger = getLogger()

CHK_FREQ = 3
NUM_OF_REQ = 14


@pytest.fixture(scope="module")
def tconf(tconf):
    limit = tconf.REPLICA_STASH_LIMIT
    tconf.REPLICA_STASH_LIMIT = 6
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf

    tconf.REPLICA_STASH_LIMIT = limit


def check_prepared(nodes):
    pre_prepared_txns = [node.master_replica._consensus_data.preprepared for node in nodes]
    prepared_txns = [node.master_replica._consensus_data.prepared for node in nodes]
    assert pre_prepared_txns == prepared_txns

    remainder = NUM_OF_REQ % CHK_FREQ
    if remainder == 0:
        prepared_txns == [[]] * len(nodes)
    else:
        start = NUM_OF_REQ - remainder + 1
        stop = NUM_OF_REQ + 1

        assert prepared_txns.count(prepared_txns[0]) == len(prepared_txns)
        assert set(range(start, stop)) == set([batch.pp_seq_no for txn in prepared_txns for batch in txn])


def test_preprepares_and_prepares_recovery_after_catchup(tdir, tconf,
                                                         looper,
                                                         testNodeClass,
                                                         txnPoolNodeSet,
                                                         sdk_pool_handle,
                                                         sdk_wallet_client,
                                                         allPluginsPath,
                                                         chkFreqPatched):
    """
    Test that all preprepares and prepares are recovered from the audit ledger after reboot.
    """

    node_to_restart = txnPoolNodeSet[-1]

    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, NUM_OF_REQ)

    # Check that all of the nodes except the slows one ordered the request
    looper.run(eventually(check_last_ordered, txnPoolNodeSet, (0, NUM_OF_REQ)))

    disconnect_node_and_ensure_disconnected(
        looper,
        txnPoolNodeSet,
        node_to_restart,
        timeout=len(txnPoolNodeSet),
        stopNode=True
    )
    looper.removeProdable(node_to_restart)
    txnPoolNodeSet.remove(node_to_restart)

    restarted_node = start_stopped_node(node_to_restart, looper, tconf, tdir, allPluginsPath)
    txnPoolNodeSet.append(restarted_node)

    looper.runFor(waits.expectedNodeStartUpTimeout())
    looper.run(checkNodesConnected(txnPoolNodeSet))

    check_prepared(txnPoolNodeSet)
