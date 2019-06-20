from logging import getLogger

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Checkpoint, CatchupRep
from plenum.common.startable import Mode
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.test import waits
from plenum.test.checkpoints.helper import chkChkpoints, chk_chkpoints_for_instance
from plenum.test.delayers import cr_delay
from plenum.test.pool_transactions.helper import \
    disconnect_node_and_ensure_disconnected
from plenum.test.helper import sdk_send_random_and_check, assertExp, max_3pc_batch_limits, \
    check_last_ordered_3pc_on_all_replicas
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


def test_3pc_while_catchup_with_chkpoints(tdir, tconf,
                                          looper,
                                          chkFreqPatched,
                                          reqs_for_checkpoint,
                                          testNodeClass,
                                          txnPoolNodeSet,
                                          sdk_pool_handle,
                                          sdk_wallet_client,
                                          allPluginsPath):
    '''
    Tests that 3PC messages and Checkpoints being ordered during catch-up are stashed and re-applied
    when catch-up is finished.
    Check that catch-up is not started again even if a quorum of stashed checkpoints
    is received.
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

    with delay_rules(lagging_node.nodeIbStasher, cr_delay(ledger_filter=DOMAIN_LEDGER_ID)):
        looper.add(lagging_node)
        txnPoolNodeSet[-1] = lagging_node
        looper.run(checkNodesConnected(txnPoolNodeSet))

        # wait till we got catchup replies for messages missed while the node was offline,
        # so that now qwe can order more messages, and they will not be caught up, but stashed
        looper.run(
            eventually(lambda: assertExp(lagging_node.nodeIbStasher.num_of_stashed(CatchupRep) >= 3), retryWait=1,
                       timeout=60))

        assert lagging_node.mode == Mode.syncing

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

        # lagging node is catching up and stashing all checkpoints
        assert lagging_node.mode == Mode.syncing
        looper.run(
            eventually(
                lambda: assertExp(get_stashed_checkpoints(lagging_node) == num_checkpoints * len(rest_nodes)),
                timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
            )
        )

    # check that last_ordered is set
    looper.run(
        eventually(check_last_ordered_3pc_on_all_replicas, [lagging_node],
                   (0, num_reqs + 2))
    )

    # check that checkpoint is stabilized for master
    looper.run(eventually(chk_chkpoints_for_instance, [lagging_node], 0, 2, 0))

    # check that the catch-up is finished
    looper.run(
        eventually(
            lambda: assertExp(lagging_node.mode == Mode.participating), retryWait=1,
            timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
        )
    )

    # check that catch-up was started only once
    looper.run(
        eventually(
            lambda: assertExp(
                lagging_node.spylog.count(Node.allLedgersCaughtUp) == initial_all_ledgers_caught_up + 1)
        )
    )
    looper.run(
        eventually(
            lambda: assertExp(
                lagging_node.spylog.count(Node.start_catchup) == 1)
        )
    )

    waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=5)


def get_stashed_checkpoints(node):
    return sum(
        1 for (stashed, sender) in node.master_replica.stasher._stashed_catch_up if isinstance(stashed, Checkpoint))
