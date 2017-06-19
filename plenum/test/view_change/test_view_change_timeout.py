import pytest
from plenum.test.delayers import reset_delays_and_process_delayeds, icDelay, delayNonPrimaries
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.test_node import get_master_primary_node, \
    ensureElectionsDone
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7

def test_view_change_timeout(nodeSet, looper, up, wallet1, client1, viewNo):
    """
    Check view change restarted if it is not completed in time
    """

    m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))

    # Setting view change timeout to low value to make test pass quicker
    for node in nodeSet:
        node._view_change_timeout = 5

    # Delaying view change messages to make first view change fail
    # due to timeout
    for node in nodeSet:
        node.nodeIbStasher.delay(icDelay(delay=5))

    # Delaying preprepae messages from nodes and
    # sending request to force view change
    for i in range(3):
        delayNonPrimaries(nodeSet, instId=i, delay=10)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    # First view change should fail, because of delayed
    # instance change messages. This then leads to new view change that we need.
    with pytest.raises(AssertionError):
        ensure_view_change(looper, nodeSet)
    with pytest.raises(AssertionError):
        ensureElectionsDone(looper=looper, nodes=nodeSet)

    # Resetting delays to let second view change go well
    reset_delays_and_process_delayeds(nodeSet)

    # This view change should be completed with no problems
    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)

    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name
    for node in nodeSet:
        assert node.spylog.count('_check_view_change_completed') > 0
    for node in nodeSet:
        assert node.viewNo == (viewNo + 2)

