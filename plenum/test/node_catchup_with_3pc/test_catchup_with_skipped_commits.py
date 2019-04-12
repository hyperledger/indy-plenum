from logging import getLogger
from typing import Iterable

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.common.messages.node_messages import Commit
from plenum.common.util import compare_3PC_keys
from plenum.server.catchup.node_leecher_service import NodeLeecherService
from plenum.test.delayers import cr_delay, delay_3pc
from plenum.test.helper import sdk_send_random_and_check, sdk_send_random_requests, sdk_get_and_check_replies, \
    max_3pc_batch_limits
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.stasher import delay_rules, start_delaying, stop_delaying_and_process
from stp_core.loop.eventually import eventually

logger = getLogger()


@pytest.fixture(scope="module")
def tconf(tconf):
    with max_3pc_batch_limits(tconf, size=1) as tconf:
        yield tconf


def test_catchup_with_skipped_commits(tdir, tconf,
                                      looper,
                                      txnPoolNodeSet,
                                      sdk_pool_handle,
                                      sdk_wallet_client):
    lagging_node = txnPoolNodeSet[-1]
    lagging_stasher = lagging_node.nodeIbStasher
    other_nodes = txnPoolNodeSet[:-1]
    other_stashers = [node.nodeIbStasher for node in other_nodes]

    def lagging_node_state() -> NodeLeecherService.State:
        return lagging_node.ledgerManager._node_leecher._state

    def check_lagging_node_is_not_syncing_audit():
        assert lagging_node_state() != NodeLeecherService.State.SyncingAudit

    def check_lagging_node_done_catchup():
        assert lagging_node_state() == NodeLeecherService.State.Idle

    def check_nodes_ordered_till(nodes: Iterable, view_no: int, pp_seq_no: int):
        for node in nodes:
            assert compare_3PC_keys((view_no, pp_seq_no), node.master_replica.last_ordered_3pc) >= 0

    # Preload nodes with some transactions
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 1)
    for node in txnPoolNodeSet:
        assert node.master_replica.last_ordered_3pc == (0, 1)

    # Setup delayers
    lagging_mid_commits = start_delaying(lagging_stasher, delay_3pc(after=3, before=6, msgs=Commit))
    others_mid_commits = start_delaying(other_stashers, delay_3pc(after=3, before=6, msgs=Commit))
    start_delaying(lagging_stasher, delay_3pc(before=4, msgs=Commit))

    # Send more requests
    reqs = sdk_send_random_requests(looper, sdk_pool_handle, sdk_wallet_client, 6)

    # Wait until pool ordered till (0, 3)
    looper.run(eventually(check_nodes_ordered_till, other_nodes, 0, 3))
    assert lagging_node.master_replica.last_ordered_3pc == (0, 1)

    with delay_rules(lagging_stasher, delay_catchup(DOMAIN_LEDGER_ID)):
        with delay_rules(lagging_stasher, delay_catchup(AUDIT_LEDGER_ID)):
            # Start catchup
            lagging_node.start_catchup()
            looper.runFor(0.5)
            assert lagging_node_state() == NodeLeecherService.State.SyncingAudit

            # Process missing commits on lagging node
            stop_delaying_and_process(lagging_mid_commits)
            looper.runFor(0.5)

        # Allow to catchup audit ledger
        looper.run(eventually(check_lagging_node_is_not_syncing_audit))
        stop_delaying_and_process(others_mid_commits)

    # Ensure that audit ledger is caught up by lagging node
    looper.run(eventually(check_lagging_node_done_catchup))

    # Ensure that all requests were ordered
    sdk_get_and_check_replies(looper, reqs)

    # Ensure that all nodes will eventually have same data
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)


def delay_catchup(ledger_id: int):
    _delayer = cr_delay(ledger_filter=ledger_id)
    _delayer.__name__ = "delay_catchup({})".format(ledger_id)
    return _delayer
