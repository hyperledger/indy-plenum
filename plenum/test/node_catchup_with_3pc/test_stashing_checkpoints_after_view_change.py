from logging import getLogger

import pytest

from plenum.common.messages.node_messages import Checkpoint
from plenum.common.startable import Mode
from plenum.server.node import Node
from plenum.server.replica import Replica
from plenum.server.replica_validator_enums import STASH_VIEW_3PC
from plenum.test import waits
from plenum.test.checkpoints.helper import check_for_nodes, check_stable_checkpoint, check_for_instance
from plenum.test.delayers import lsDelay, nv_delay
from plenum.test.helper import sdk_send_random_and_check, assertExp, max_3pc_batch_limits, \
    check_last_ordered_3pc_on_master, check_last_ordered_3pc_on_backup
from plenum.test.node_catchup.helper import waitNodeDataEquality
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change
from stp_core.loop.eventually import eventually

from plenum.test.checkpoints.conftest import chkFreqPatched, reqs_for_checkpoint

logger = getLogger()

CHK_FREQ = 5


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def test_checkpoints_after_view_change(tconf,
                                       looper,
                                       chkFreqPatched,
                                       reqs_for_checkpoint,
                                       txnPoolNodeSet,
                                       sdk_pool_handle,
                                       sdk_wallet_client):
    '''
    Tests that there is no infinite catchups if there is
    a quorum of stashed checkpoints received during the view change
    '''

    # Prepare nodes
    lagging_node = txnPoolNodeSet[-1]
    rest_nodes = txnPoolNodeSet[:-1]

    initial_all_ledgers_caught_up = lagging_node.spylog.count(Node.allLedgersCaughtUp)
    initial_start_catchup = lagging_node.spylog.count(Node.start_catchup)

    with delay_rules(lagging_node.nodeIbStasher, lsDelay()):
        with delay_rules(lagging_node.nodeIbStasher, nv_delay()):
            ensure_view_change(looper, txnPoolNodeSet)
            looper.run(
                eventually(
                    lambda: assertExp(lagging_node.view_change_in_progress is True),
                    timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
                )
            )
            ensureElectionsDone(looper=looper, nodes=rest_nodes, instances_list=range(2))

            assert all(n.view_change_in_progress is False for n in rest_nodes)
            assert lagging_node.view_change_in_progress is True

            # make sure that more requests are being ordered while catch-up is in progress on the lagging node
            # stash enough stable checkpoints for starting a catch-up
            num_checkpoints = Replica.STASHED_CHECKPOINTS_BEFORE_CATCHUP + 1
            num_reqs = reqs_for_checkpoint * num_checkpoints + 1
            sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                      sdk_wallet_client,
                                      num_reqs)
            looper.run(
                eventually(check_last_ordered_3pc_on_master, rest_nodes,
                           (1, num_reqs + 1))
            )
            looper.run(
                eventually(check_last_ordered_3pc_on_backup, rest_nodes,
                           (1, num_reqs + 1))
            )

            # all good nodes stabilized checkpoint
            looper.run(eventually(check_for_nodes, rest_nodes, check_stable_checkpoint, 10))

            assert get_stashed_checkpoints(lagging_node) == num_checkpoints * len(rest_nodes)
            # lagging node is doing the view change and stashing all checkpoints
            assert lagging_node.view_change_in_progress is True
            looper.run(
                eventually(
                    lambda: assertExp(get_stashed_checkpoints(lagging_node) == 2 * len(rest_nodes)),
                    timeout=waits.expectedPoolCatchupTime(len(txnPoolNodeSet))
                )
            )

    # check that view change is finished
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    assert lagging_node.view_change_in_progress is False

    # check that last_ordered is set
    looper.run(
        eventually(check_last_ordered_3pc_on_master, [lagging_node],
                   (1, num_reqs + 1))
    )
    looper.run(
        eventually(check_last_ordered_3pc_on_backup, [lagging_node],
                   (1, num_reqs + 1))
    )

    # check that checkpoint is stabilized for master
    looper.run(eventually(check_for_instance, [lagging_node], 0, check_stable_checkpoint, 10))

    # check that the catch-up didn't happen
    assert lagging_node.mode == Mode.participating
    assert lagging_node.spylog.count(Node.allLedgersCaughtUp) == initial_all_ledgers_caught_up
    assert lagging_node.spylog.count(Node.start_catchup) == initial_start_catchup

    waitNodeDataEquality(looper, *txnPoolNodeSet, customTimeout=5)


def get_stashed_checkpoints(node):
    return sum(
        1 for (stashed, sender) in node.master_replica.stasher._queues[STASH_VIEW_3PC] if isinstance(stashed, Checkpoint))
