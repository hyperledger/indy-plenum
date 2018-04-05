import types

import pytest

from plenum.test.helper import checkViewNoForNodes, \
    sdk_send_random_and_check, countDiscarded
from plenum.test.malicious_behaviors_node import slow_primary
from plenum.test.test_node import getPrimaryReplica, ensureElectionsDone
from plenum.test.view_change.helper import provoke_and_wait_for_view_change, ensure_view_change

from stp_core.common.log import getlogger

logger = getlogger()


def test_master_primary_different_from_previous(txnPoolNodeSet, looper,
                                                sdk_pool_handle, sdk_wallet_client):
    """
    After a view change, primary must be different from previous primary for
    master instance, it does not matter for other instance. The primary is
    benign and does not vote for itself.
    """
    pr = slow_primary(txnPoolNodeSet, 0, delay=10)
    old_pr_node_name = pr.node.name

    # View change happens
    ensure_view_change(looper, txnPoolNodeSet)
    logger.debug("VIEW HAS BEEN CHANGED!")

    # Elections done
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    # New primary is not same as old primary
    assert getPrimaryReplica(txnPoolNodeSet, 0).node.name != old_pr_node_name

    pr.outBoxTestStasher.resetDelays()

    # The new primary can still process requests
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, 5)


@pytest.mark.skip(reason='Nodes use round robin primary selection')
def test_master_primary_different_from_previous_view_for_itself(
        txnPoolNodeSet, looper, sdk_pool_handle, sdk_wallet_client):
    """
    After a view change, primary must be different from previous primary for
    master instance, it does not matter for other instance. Break it into
    2 tests, one where the primary is malign and votes for itself but is still
    not made primary in the next view.
    """
    old_view_no = checkViewNoForNodes(txnPoolNodeSet)
    pr = slow_primary(txnPoolNodeSet, 0, delay=10)
    old_pr_node = pr.node

    def _get_undecided_inst_id(self):
        undecideds = [i for i, r in enumerate(self.replicas)
                      if r.isPrimary is None]
        # Try to nominate for the master instance
        return undecideds, 0

    # Patching old primary's elector's method to nominate itself
    # again for the the new view
    old_pr_node.elector._get_undecided_inst_id = types.MethodType(
        _get_undecided_inst_id, old_pr_node.elector)

    # View change happens
    provoke_and_wait_for_view_change(looper,
                                     txnPoolNodeSet,
                                     old_view_no + 1,
                                     sdk_pool_handle,
                                     sdk_wallet_client)

    # Elections done
    ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # New primary is not same as old primary
    assert getPrimaryReplica(txnPoolNodeSet, 0).node.name != old_pr_node.name

    # All other nodes discarded the nomination by the old primary
    for node in txnPoolNodeSet:
        if node != old_pr_node:
            assert countDiscarded(node.elector,
                                  'of master in previous view too') == 1

    # The new primary can still process requests
    sdk_send_random_and_check(looper, txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              5)
