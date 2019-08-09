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
