import types
from _ast import Dict, List
from collections import Sequence

import pytest
import sys

from plenum.common.messages.node_messages import Prepare, PrePrepare, Commit
from plenum.server.node import Node
from plenum.test.delayers import icDelay, cDelay, pDelay
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_replies
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually
from stp_core.loop.exceptions import EventuallyTimeoutException

fast_node = "Gamma"


def test_view_change_with_different_prepared_certificate(txnPoolNodeSet, looper,
                                                         sdk_pool_handle,
                                                         sdk_wallet_client,
                                                         tconf,
                                                         viewNo,
                                                         monkeypatch):
    # 1. Send some txns
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)

    def start_view_change(commit: Commit):
        # if get_count(replica, replica._request_three_phase_msg) > 1:
        for node in txnPoolNodeSet:
            key = (commit.viewNo, commit.ppSeqNo)
            node.view_changer.startViewChange(
                replica.node.viewNo + 1)
            monkeypatch.delattr(node.replicas[0], "canOrder")
        return False, ""

    for node in txnPoolNodeSet:
        replica = node.replicas[0]
        monkeypatch.setattr(replica, 'canOrder', start_view_change)

    requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                        sdk_wallet_client, 2)
    sdk_get_replies(looper, requests)


def tmp(prepare: Prepare, sender: str):
    print("test")


def test_view_change_in_different_time(txnPoolNodeSet, looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       tconf,
                                       monkeypatch):
    """Test case:
    given 4 nodes
    disable normal view change to make tests deterministic
    indefinitely delay receiving commit messages on all nodes
    send some requests
    wait until all nodes have same last prepare certificate
    trigger view change on all nodes (using view_changer.on_master_degradation)
    stop delaying commits on two nodes
    wait until view change is complete
    stop delaying commits on two other nodes
    try ordering transactions
    Expected result with correct view change: transactions should be ordered normally
    Expected result with current view change: transactions won't be ordered because pool is in inconsistent state
    """

    first_two_nodes = txnPoolNodeSet[:2]
    other_two_nodes = txnPoolNodeSet[2:]
    first_two_nodes_stashers = [n.nodeIbStasher for n in first_two_nodes]
    other_two_nodes_stashers = [n.nodeIbStasher for n in other_two_nodes]

    with delay_rules(first_two_nodes_stashers, cDelay()):
        with delay_rules(other_two_nodes_stashers, cDelay()):
            requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                                sdk_wallet_client, 1)

            def view_change_done(nodes: [Node], view_no):
                for node in nodes:
                    assert node.viewNo == view_no

            looper.run(eventually(prepare_certificate, txnPoolNodeSet, 1))

            for node in txnPoolNodeSet:
                node.view_changer.on_master_degradation()

            # Wait for view change done on other two nodes
            # looper.run(eventually(view_change_done, other_two_nodes,
            #                       retryWait=1, timeout=100))

        #           ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

        # Wait for view change done one first two nodes
        looper.run(eventually(view_change_done, txnPoolNodeSet,
                              retryWait=1, timeout=100))

    #        ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)

    sdk_get_replies(looper, requests)

    # Send requests, we should fail here :)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)


def test_delay_commits(txnPoolNodeSet, looper,
                       sdk_pool_handle,
                       sdk_wallet_client,
                       tconf,
                       monkeypatch):
    tconf.unsafe.add("disable_view_change")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    nodes_stashers = [n.nodeIbStasher for n in txnPoolNodeSet
                      if n.name != "Delta"]
    delay_commits_for_all_except_one_node(txnPoolNodeSet,
                                          nodes_stashers,
                                          txnPoolNodeSet[-1],
                                          looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client)
    nodes_stashers = [n.nodeIbStasher for n in txnPoolNodeSet
                      if n.name != "Gamma"]
    delay_commits_for_all_except_one_node(txnPoolNodeSet,
                                          nodes_stashers,
                                          txnPoolNodeSet[-2],
                                          looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client)

    # sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
    #                           sdk_wallet_client, 4)
    last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    for node in txnPoolNodeSet:
        assert node.master_replica.last_ordered_3pc == last_ordered


def delay_commits_for_all_except_one_node(nodes, nodes_stashers,
                                          except_node,
                                          looper,
                                          sdk_pool_handle,
                                          sdk_wallet_client):
    new_view_no = except_node.viewNo + 1
    old_last_ordered = except_node.master_replica.last_ordered_3pc
    # indefinitely delay receiving commit messages on first_nodes
    with delay_rules(nodes_stashers, cDelay(sys.maxsize)):
        # send one more request
        requests2 = sdk_send_random_requests(looper, sdk_pool_handle,
                                             sdk_wallet_client, 1)

        def last_ordered(node: Node, last_ordered):
            assert node.master_replica.last_ordered_3pc == last_ordered

        # wait until other two nodes increase last prepare certificate
        looper.run(
            eventually(last_ordered, except_node, (except_node.viewNo, old_last_ordered[1])))

        # trigger view change on all nodes
        for node in nodes:
            node.view_changer.on_master_degradation()

        # wait for view change done on all nodes
        looper.run(eventually(view_change_done, nodes, new_view_no))

    sdk_get_replies(looper, requests2)


def test_view_change_with_delay_commits_and_prepares(txnPoolNodeSet, looper,
                                                     sdk_pool_handle,
                                                     sdk_wallet_client,
                                                     tconf):
    '''
    Test case:
    given 4 nodes
    increase view change timeout to exceed allowed test running time
    disable normal view change to make tests deterministic
    indefinitely delay receiving commit messages on all nodes
    send some requests
    wait until all nodes have same last prepare certificate
    indefinitely delay receiving prepare messages on two nodes
    send one more request
    wait until other two nodes increase last prepare certificate
    trigger view change on all nodes (using view_changer.on_master_degradation)
    stop delaying commits
    wait for view change done on all nodes
    Expected result with correct view change: view change will be successfully finished
    Expected result with current view change: view change will take maximum time allowed for view change, triggering test timeout
    :return:
    '''

    # increase view change timeout to exceed allowed test running time
    view_change_timeout = 200  # default=60
    for node in txnPoolNodeSet:
        node._view_change_timeout = view_change_timeout
    # disable normal view change to make tests deterministic
    tconf.unsafe.add("disable_view_change")

    first_two_nodes = txnPoolNodeSet[:2]
    other_two_nodes = txnPoolNodeSet[2:]
    first_two_nodes_stashers = [n.nodeIbStasher for n in first_two_nodes]
    nodes_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    # indefinitely delay receiving commit messages on all nodes
    with delay_rules(nodes_stashers, cDelay(sys.maxsize)):
        # send some requests
        requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                            sdk_wallet_client, 1)

        # wait until all nodes have same last prepare certificate
        looper.run(eventually(last_prepared_certificate, other_two_nodes,
                              (0, 1)))

        # indefinitely delay receiving prepare messages on two nodes
        with delay_rules(first_two_nodes_stashers, pDelay(sys.maxsize)):
            # send one more request
            requests2 = sdk_send_random_requests(looper, sdk_pool_handle,
                                                 sdk_wallet_client, 1)

            # wait until other two nodes increase last prepare certificate
            looper.run(eventually(last_prepared_certificate, other_two_nodes,
                                  (0, 2)))

            # trigger view change on all nodes (using view_changer.on_master_degradation)
            for node in txnPoolNodeSet:
                node.view_changer.on_master_degradation()
            # stop delaying commits

    # wait for view change done on all nodes
    looper.run(eventually(view_change_done, txnPoolNodeSet, 1))
    sdk_get_replies(looper, requests)
    sdk_get_replies(looper, requests2)


def test_view_change_tmp(txnPoolNodeSet, looper,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         tconf):
    '''
    Test case:
    given 4 nodes
    increase view change timeout to exceed allowed test running time
    disable normal view change to make tests deterministic
    indefinitely delay receiving commit messages on all nodes
    send some requests
    wait until all nodes have same last prepare certificate
    indefinitely delay receiving prepare messages on two nodes
    send one more request
    wait until other two nodes increase last prepare certificate
    trigger view change on all nodes (using view_changer.on_master_degradation)
    stop delaying commits
    wait for view change done on all nodes
    Expected result with correct view change: view change will be successfully finished
    Expected result with current view change: view change will take maximum time allowed for view change, triggering test timeout
    :return:
    '''

    # increase view change timeout to exceed allowed test running time

    # disable normal view change to make tests deterministic
    tconf.unsafe.add("disable_view_change")
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    first_two_nodes = txnPoolNodeSet[:3]
    other_two_nodes = txnPoolNodeSet[3:]
    first_two_nodes_stashers = [n.nodeIbStasher for n in first_two_nodes]
    nodes_stashers = [n.nodeIbStasher for n in txnPoolNodeSet]

    # indefinitely delay receiving commit messages on all nodes
    with delay_rules(nodes_stashers, cDelay(sys.maxsize)):

        # indefinitely delay receiving prepare messages on two nodes
        with delay_rules(first_two_nodes_stashers, pDelay(sys.maxsize)):
            # send one more request
            requests2 = sdk_send_random_requests(looper, sdk_pool_handle,
                                                 sdk_wallet_client, 1)

            # wait until other two nodes increase last prepare certificate
            looper.run(
                eventually(last_prepared_certificate, other_two_nodes, (0, 2)))

            # trigger view change on all nodes (using view_changer.on_master_degradation)
            for node in txnPoolNodeSet:
                node.view_changer.on_master_degradation()

            # wait for view change done on all nodes

            def view_change_done(nodes: [Node], viewNo):
                for node in nodes:
                    assert node.viewNo == viewNo
                    assert node.master_replica.last_prepared_before_view_change is None

            looper.run(eventually(view_change_done, txnPoolNodeSet, 1))

        for node in txnPoolNodeSet:
            print("\n!" + str(
                node.master_replica.last_prepared_certificate_in_view()) + " vN: " + str(
                node.viewNo))

    sdk_get_replies(looper, requests2)
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)


def prepare_certificate(nodes: [Node]):
    for node in nodes:
        replica = node.replicas[0]
        (last_pp_view_no, last_pp_seq_no) = replica.last_ordered_3pc
        tmp = replica.last_prepared_certificate_in_view()
        assert tmp == (replica.viewNo, last_pp_seq_no + 1)


def last_prepared_certificate(nodes, num):
    for n in nodes:
        assert n.master_replica.last_prepared_certificate_in_view() == num


def view_change_done(nodes: [Node], viewNo):
    for node in nodes:
        assert node.viewNo == viewNo
        assert node.master_replica.last_prepared_before_view_change is None
