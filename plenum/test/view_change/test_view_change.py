from plenum.test.delayers import delayNonPrimaries, \
    reset_delays_and_process_delayeds
from plenum.test.helper import waitForViewChange, \
    sendReqsToNodesAndVerifySuffReplies
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.test_node import get_master_primary_node, \
    ensureElectionsDone, checkProtocolInstanceSetup
from plenum.test.delayers import icDelay
from plenum.test.view_change.helper import ensure_view_change

nodeCount = 7

# noinspection PyIncorrectDocstring
def test_view_change_on_empty_ledger(nodeSet, up, looper):

    """
    Check that view change is done when no txns in the ldegr
    """
    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

# noinspection PyIncorrectDocstring
def test_view_change_after_some_txns(looper, nodeSet, up, viewNo,
                   wallet1, client1):
    """
    Check that view change is done after processing some of txns
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

# noinspection PyIncorrectDocstring
def test_send_more_after_view_change(looper, nodeSet, up, viewNo,
                   wallet1, client1):
    """
    Check that we can send more requests after view change
    """
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)

    ensure_view_change(looper, nodeSet)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 10)


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
    checkProtocolInstanceSetup(looper, nodeSet, retryWait=1)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)

    new_m_primary_node = get_master_primary_node(list(nodeSet.nodes.values()))
    assert m_primary_node.name != new_m_primary_node.name
    for node in nodeSet:
        assert node.spylog.count('_check_view_change_completed') > 0
    for node in nodeSet:
        assert node.viewNo == (viewNo + 2)


def test_node_notified_about_primary_election_result(nodeSet, looper, up,
                                                     wallet1, client1, viewNo):
    delayNonPrimaries(nodeSet, 0, 10)
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, client1, 4)
    waitForViewChange(looper, nodeSet, expectedViewNo=viewNo + 1)
    ensureElectionsDone(looper=looper, nodes=nodeSet)
    ensure_all_nodes_have_same_data(looper, nodes=nodeSet)
    for node in nodeSet:
        assert node.spylog.count('primary_found') > 0
