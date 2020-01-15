import pytest
import sys

from plenum.server.node import Node
from plenum.test.delayers import cDelay
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_replies, perf_monitor_disabled
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.stasher import delay_rules
from plenum.test.view_change_service.helper import trigger_view_change
from stp_core.loop.eventually import eventually


@pytest.fixture(scope="module")
def tconf(tconf):
    with perf_monitor_disabled(tconf):
        yield tconf


def test_delay_commits(txnPoolNodeSet, looper,
                       sdk_pool_handle,
                       sdk_wallet_client,
                       tconf):
    """
    #3

    Test case:

    disable normal view change to make tests deterministic
    delay commits for all nodes except node X
    send request
    check ordered transaction in node X
    start view_change
    check end of view change for all nodes
    switch off commits' delay
    get reply (means that request was ordered in all nodes)
    repeat
    Expected result with correct view change:
    transactions should be ordered normally
    Expected result with current view change:
    node X can't finish second transaction
    """
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    nodes_stashers = [n.nodeIbStasher for n in txnPoolNodeSet
                      if n != txnPoolNodeSet[-1]]
    for _ in range(2):
        do_view_change_with_delayed_commits_on_all_but_one(txnPoolNodeSet,
                                              nodes_stashers,
                                              txnPoolNodeSet[-1],
                                              looper,
                                              sdk_pool_handle,
                                              sdk_wallet_client)


def do_view_change_with_delayed_commits_on_all_but_one(nodes, nodes_without_one_stashers,
                                          except_node,
                                          looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client):
    new_view_no = except_node.viewNo + 1
    old_last_ordered = except_node.master_replica.last_ordered_3pc
    # delay commits for all nodes except node X
    with delay_rules(nodes_without_one_stashers, cDelay(sys.maxsize)):
        # send one  request
        requests2 = sdk_send_random_requests(looper, sdk_pool_handle,
                                             sdk_wallet_client, 1)

        def last_ordered(node: Node, last_ordered):
            assert node.master_replica.last_ordered_3pc == last_ordered

        # wait until except_node ordered txn
        looper.run(
            eventually(last_ordered, except_node, (except_node.viewNo,
                                                   old_last_ordered[1] + 1)))

        # trigger view change on all nodes
        trigger_view_change(nodes)

        # wait for view change done on all nodes
        looper.run(eventually(view_change_done, nodes, new_view_no))

    sdk_get_replies(looper, requests2)
    ensure_all_nodes_have_same_data(looper, nodes)
    sdk_ensure_pool_functional(looper, nodes, sdk_wallet_client, sdk_pool_handle)


def last_prepared_certificate(nodes, num):
    for n in nodes:
        assert n.master_replica.last_prepared_certificate_in_view() == num


def view_change_done(nodes: [Node], view_no):
    for node in nodes:
        assert node.viewNo == view_no
