from stp_core.loop.looper import Looper
from plenum.test.test_node import TestNodeSet
from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies


def test_not_check_if_no_new_requests(looper: Looper,
                                      nodeSet: TestNodeSet,
                                      wallet1, client1):
    """
    Checks that node does not do performance check if there were no new
    requests since previous check
    """
    
    # Ensure that nodes participating, because otherwise they do not do check
    for node in list(nodeSet):
        assert node.isParticipating

    # Check that first performance checks passes, but further do not
    for node in list(nodeSet):
        assert node.checkPerformance()
        assert not node.checkPerformance()
        assert not node.checkPerformance()
        assert not node.checkPerformance()

    # Send new request and check that after it nodes can do
    # performance check again
    num_requests = 1
    sendReqsToNodesAndVerifySuffReplies(looper,
                                        wallet1,
                                        client1,
                                        num_requests,
                                        nodeSet.f)

    for node in list(nodeSet):
        assert node.checkPerformance()
