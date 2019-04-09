from logging import getLogger

import pytest

from plenum.common.messages.node_messages import Checkpoint, LedgerStatus
from plenum.common.startable import Mode
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints
from plenum.test.delayers import cs_delay, lsDelay, \
    ppDelay, pDelay, cDelay, msg_rep_delay, cr_delay
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sdk_send_random_and_check, assertExp, max_3pc_batch_limits, \
    check_last_ordered_3pc_on_all_replicas, check_last_ordered_3pc_on_master
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import checkNodesConnected
from plenum.test.view_change.helper import start_stopped_node
from stp_core.loop.eventually import eventually

from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint

logger = getLogger()

CHK_FREQ = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def test_3pc_while_catchup_with_chkpoints_only(tdir, tconf,
                                               looper,
                                               chkFreqPatched,
                                               reqs_for_checkpoint,
                                               testNodeClass,
                                               txnPoolNodeSet,
                                               sdk_pool_handle,
                                               sdk_wallet_client,
                                               allPluginsPath):
    '''
    Check that catch-up is not started again even if a quorum of stashed checkpoints
    is received during catch-up.
    Assume that only checkpoints and no 3PC messages are received.
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

    # Send more requests to active nodes
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
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

    # delay all 3PC messages on the lagged node so that it
    # receives only Checkpoints and catch-up messages

    lagging_node.nodeIbStasher.delay(ppDelay())
    lagging_node.nodeIbStasher.delay(pDelay())
    lagging_node.nodeIbStasher.delay(cDelay())

    with delay_rules(lagging_node.nodeIbStasher, lsDelay(), cr_delay()):
        looper.add(lagging_node)
        txnPoolNodeSet[-1] = lagging_node
        looper.run(checkNodesConnected(txnPoolNodeSet))

        # wait till we got ledger statuses for messages missed while the node was offline,
        # so that now we can order more messages, and they will not be caught up, but stashed
        looper.run(
            eventually(lambda: assertExp(lagging_node.nodeIbStasher.num_of_stashed(LedgerStatus) >= 3), retryWait=1,
                       timeout=60))

        assert lagging_node.mode != Mode.participating

        # make sure that more requests are being ordered while catch-up is in progress
        # stash enough stable checkpoints for starting a catch-up
        num_checkpoints = Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1
        num_reqs = reqs_for_checkpoint * num_checkpoints + 1
        sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                  sdk_wallet_client,
                                  num_reqs)
        looper.run(
            eventually(check_last_ordered_3pc_on_all_replicas, rest_nodes,
                       (0, num_reqs + 2))
        )

        # all good nodes stabilized checkpoint
        looper.run(eventually(chkChkpoints, rest_nodes, 2, 0))

        assert lagging_node.mode != Mode.participating
        # lagging node is catching up and stashing all checkpoints
        looper.run(
            eventually(
                lambda: assertExp(get_stashed_checkpoints(lagging_node) == num_checkpoints * len(rest_nodes)),
                timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
            )
        )

    # check that last_ordered is set
    looper.run(
        eventually(check_last_ordered_3pc_on_master, [lagging_node],
                   (0, num_reqs + 2))
    )

    # check that the catch-up is finished
    looper.run(
        eventually(
            lambda: assertExp(lagging_node.mode == Mode.participating), retryWait=1,
            timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
        )
    )

    # check that catch-up was started twice, since we were able to catch-up till audit ledger only
    # for the first time, and after this the node sees a quorum of stashed checkpoints
    assert lagging_node.spylog.count(Node.allLedgersCaughtUp) == initial_all_ledgers_caught_up + 1
    assert lagging_node.spylog.count(Node.start_catchup) == 1

    waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=5)


def get_stashed_checkpoints(node):
    return sum(
        1 for (stashed, sender) in node.master_replica.stasher._stashed_catch_up if isinstance(stashed, Checkpoint))
