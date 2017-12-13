import types

import pytest
from plenum.test.helper import waitForViewChange, sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.primary_selection.test_primary_selection_pool_txn import \
    ensure_pool_functional
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.exceptions import EventuallyTimeoutException

nodeCount = 7


def patch_has_ordered_till_last_prepared_certificate(nodeSet):
    def patched_has_ordered_till_last_prepared_certificate(self) -> bool:
        return False

    for node in nodeSet:
        node.has_ordered_till_last_prepared_certificate = \
            types.MethodType(
                patched_has_ordered_till_last_prepared_certificate, node)


def test_view_change_min_catchup_timeout(nodeSet, up, looper, wallet1, client1,
                                         tconf,
                                         viewNo):
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
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    # 2. make the only condition to finish catch-up is
    # MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    patch_has_ordered_till_last_prepared_certificate(nodeSet)

    # 3. start view change
    expected_view_no = viewNo + 1
    for node in nodeSet:
        node.view_changer.startViewChange(expected_view_no)

    # 4. check that it's not finished till
    # MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE
    no_view_chanage_timeout = tconf.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE - 1
    with pytest.raises(EventuallyTimeoutException):
        ensureElectionsDone(looper=looper, nodes=nodeSet,
                            customTimeout=no_view_chanage_timeout)

    # 5. make sure that view change is finished eventually
    # (it should be finished quite soon after we waited for MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE)
    ensureElectionsDone(looper=looper, nodes=nodeSet, customTimeout=2)
    waitForViewChange(looper=looper, nodeSet=nodeSet,
                      expectedViewNo=expected_view_no)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

    # 6. ensure that the pool is still functional.
    ensure_pool_functional(looper, nodeSet, wallet1, client1)
