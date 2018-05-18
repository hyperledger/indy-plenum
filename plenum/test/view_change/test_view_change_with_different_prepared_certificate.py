import types

import pytest

from plenum.common.messages.node_messages import Prepare, PrePrepare, Commit
from plenum.test.helper import waitForViewChange, sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_replies
from plenum.test.node_request.helper import sdk_ensure_pool_functional
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.spy_helpers import get_count
from plenum.test.test_node import ensureElectionsDone
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
            if node.replicas[0].prePrepares.contains(key):
                node.view_changer.startViewChange(
                    replica.node.viewNo + 1)
                monkeypatch.delattr(node.replicas[0], "doOrder")

    for node in txnPoolNodeSet:
        replica = node.replicas[0]
        monkeypatch.setattr(replica, 'doOrder', start_view_change)

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
