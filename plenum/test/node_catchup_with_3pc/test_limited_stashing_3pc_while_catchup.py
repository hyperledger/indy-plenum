from logging import getLogger

import pytest

from plenum.common.constants import PREPARE, PREPREPARE, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import CatchupRep
from plenum.server.node import Node
from plenum.test import waits
from plenum.test.delayers import cr_delay, msg_rep_delay
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sdk_send_random_and_check, assertExp, max_3pc_batch_limits, \
    sdk_send_batches_of_random_and_check, check_last_ordered_3pc_on_master
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually
from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint

logger = getLogger()

CHK_FREQ = 1


@pytest.fixture(scope="module")
def tconf(tconf):
    limit = tconf.REPLICA_STASH_LIMIT
    tconf.REPLICA_STASH_LIMIT = 6
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf

    tconf.REPLICA_STASH_LIMIT = limit


def test_limited_stash_3pc_while_catchup(tdir, tconf,
                                         looper,
                                         testNodeClass,
                                         txnPoolNodeSet,
                                         sdk_pool_handle,
                                         sdk_wallet_client,
                                         allPluginsPath,
                                         chkFreqPatched):
    '''
    Test that the lagging_node can process messages from catchup stash after catchup
    and request lost messages from other nodes.
    '''

    # Prepare nodes
    lagging_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]

    # Check that requests executed well
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)

    # Stop one node
    waitNodeDataEquality(looper, lagging_node, *rest_nodes)
    disconnect_node_and_ensure_disconnected(looper,
                                            txnPoolNodeSet,
                                            lagging_node,
                                            stopNode=True)
    looper.removeProdable(lagging_node)

    # Order 2 checkpoints on rest_nodes (2 txns in 2 batches)
    sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                         sdk_pool_handle, sdk_wallet_client,
                                         2 * CHK_FREQ, 2)
    waitNodeDataEquality(looper, *rest_nodes)

    # Restart stopped node and wait for successful catch up
    lagging_node = start_stopped_node(lagging_node,
                                      looper,
                                      tconf,
                                      tdir,
                                      allPluginsPath,
                                      start=False,
                                      )

    initial_all_ledgers_caught_up = lagging_node.spylog.count(Node.allLedgersCaughtUp)

    with delay_rules(lagging_node.nodeIbStasher, msg_rep_delay(types_to_delay=[PREPARE, PREPREPARE])):
        with delay_rules(lagging_node.nodeIbStasher, cr_delay(ledger_filter=DOMAIN_LEDGER_ID)):
            looper.add(lagging_node)
            txnPoolNodeSet[-1] = lagging_node
            looper.run(checkNodesConnected(txnPoolNodeSet))

            # wait till we got catchup replies for messages missed while the node was offline,
            # so that now we can order more messages, and they will not be caught up, but stashed
            looper.run(
                eventually(lambda: assertExp(lagging_node.nodeIbStasher.num_of_stashed(CatchupRep) >= 3), retryWait=1,
                           timeout=60))

            # Order 2 checkpoints in the first lagging node catchup (2 txns in 2 batches)
            sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                                 sdk_pool_handle, sdk_wallet_client,
                                                 2 * CHK_FREQ, 2)

        # Order 2 checkpoints in the second lagging node catchup (2 txns in 2 batches)
        sdk_send_batches_of_random_and_check(looper, txnPoolNodeSet,
                                             sdk_pool_handle, sdk_wallet_client,
                                             2 * CHK_FREQ, 2)

    waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=5,
                         exclude_from_check=['check_last_ordered_3pc_backup'])
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    looper.run(
        eventually(
            lambda: assertExp(
                lagging_node.master_last_ordered_3PC == n.master_last_ordered_3PC
                for n in txnPoolNodeSet)
        )
    )

    # check that catch-up was started only twice
    assert lagging_node.spylog.count(Node.allLedgersCaughtUp) == initial_all_ledgers_caught_up + 2
