import types
from _ast import Dict, List
from collections import Sequence

import pytest

from plenum.common.messages.node_messages import Prepare, PrePrepare, Commit
from plenum.server.node import Node
from plenum.test.delayers import icDelay, cDelay
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
    """
    One of the conditions to finish catch-up during view change is to have MAX_CATCHUPS_DONE_DURING_VIEW_CHANGE
    rounds of catch-up without any new transactions caught up.
    But this should not finish very quickly.
    So, we should try to catch-up until MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE.

    In the test:
    - Before starting view change, mock `has_ordered_till_last_prepared_certificate` so that it always returns False.
    - This means that the only condition on how we can finish catch-up is by MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    timeout and having more than MAX_CATCHUPS_DONE_DURING_VIEW_CHANGE rounds of catch-up without new txns caught up.
     - Check that view change is not finished until MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
     - Check that view change is eventually finished after MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    """

    # 1. Send some txns
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)
    tryOrderCount = 0

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

    # ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    # for node in txnPoolNodeSet:
    #     if node.name != fast_node:
    #         replica = node.replicas[0]
    #         monkeypatch.delattr(replica, "tryOrder")
    sdk_get_replies(looper, requests)
    #
    # # 4. check that it's not finished till
    # # MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    # no_view_chanage_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE - 1
    # with pytest.raises(EventuallyTimeoutException):
    #     ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet,
    #                         customTimeout=no_view_chanage_timeout)
    #
    # # 5. make sure that view change is finished eventually
    # # (it should be finished quite soon after we waited for MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE)
    # ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet, customTimeout=2)
    # waitForViewChange(looper=looper, txnPoolNodeSet=txnPoolNodeSet,
    #                   expectedViewNo=expected_view_no)
    # ensure_all_nodes_have_same_data(looper, nodes=txnPoolNodeSet)
    #
    # # 6. ensure that the pool is still functional.
    # sdk_ensure_pool_functional(looper, txnPoolNodeSet,
    #                            sdk_wallet_client,
    #                            sdk_pool_handle)


def tmp(prepare: Prepare, sender: str):
    print("test")


def test_view_change_in_different_time(txnPoolNodeSet, looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       tconf,
                                       monkeypatch):

    view_no = 1
    delay = 3

    first_two_nodes = [n.nodeIbStasher for n in txnPoolNodeSet[:2]]
    other_two_nodes = [n.nodeIbStasher for n in txnPoolNodeSet[2:]]


    with delay_rules(first_two_nodes, cDelay()):
        with delay_rules(other_two_nodes, cDelay()):
            requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                                sdk_wallet_client, 2)

            def prepare_certificate(nodes: [Node]):
                prepare_cert_count = 0
                for node in nodes:
                    replica = node.replicas[0]
                    tmp = replica.last_prepared_certificate_in_view()
                    assert tmp == (replica.viewNo,)

            looper.run(eventually(prepare_certificate, txnPoolNodeSet,
                       retryWait=1, timeout=100))

            for node in txnPoolNodeSet:
                node.view_changer.on_master_degradation()

            # Wait for view change done on other two nodes
            ensureElectionsDone(looper=looper, nodes=other_two_nodes)

        # Wait for view change done one first two nodes
        ensureElectionsDone(looper=looper, nodes=first_two_nodes)

    # Send requests, we should fail here :)
    sdk_get_replies(looper, requests)


    # for node in txnPoolNodeSet:
    #     node.nodeIbStasher.delay(cDelay(delay))
    #
    # stashers = (n.nodeIbStasher for n in txnPoolNodeSet[2:3])
    # delay_rules(stashers, icDelay())
    #
    # stashers = (n.nodeIbStasher for n in txnPoolNodeSet[0:1])
    # delay_rules(stashers, cDelay())


def test_delay_commits(txnPoolNodeSet, looper,
                                       sdk_pool_handle,
                                       sdk_wallet_client,
                                       tconf,
                                       monkeypatch):
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 4)

    view_no = 1
    delay = 3
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(cDelay(delay))

    # sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
    #                           sdk_wallet_client, 1, total_timeout=100,
    #                           customTimeoutPerReq=100, add_delay_to_timeout=100)
    # ensureElectionsDone(looper=looper, nodes=txnPoolNodeSet)
    requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                        sdk_wallet_client, 2)
    for node in txnPoolNodeSet[0:2]:
        node.view_changer.sendInstanceChange(view_no)

    tmp = sdk_get_replies(looper, requests)
    print(tmp)
