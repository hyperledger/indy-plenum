from plenum.test.client.conftest import passThroughReqAcked1

from plenum.test.helper import sendReqsToNodesAndVerifySuffReplies
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes
from plenum.test.node_catchup.helper import waitNodeDataEquality


nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1


def testReplyWhenRequestSentToMoreThanFPlusOneNodes(looper, nodeSet,
                                                    fClient, replied1,
                                                    wallet1):
    """
    Alpha would not be sent request but other nodes will be, so Alpha will
    just rely on propagates from other nodes
    """
    alpha = nodeSet.Alpha
    other_nodes = [n for n in nodeSet if n != alpha]

    def chk(req_count=1):
        for node in nodeSet:
            prc_req = node.processRequest.__name__
            prc_ppg = node.processPropagate.__name__
            if node != alpha:
                # All nodes except alpha will receive requests from client
                assert node.spylog.count(prc_req) == req_count
            else:
                # Alpha will not receive requests from client
                assert node.spylog.count(prc_req) == 0

            # All nodes will get propagates from others
            assert node.spylog.count(prc_ppg) == req_count * (nodeCount - 1)

    # Ledger is same for all nodes
    waitNodeDataEquality(looper, alpha, *other_nodes)
    chk(1)

    more_reqs_count = 5
    sendReqsToNodesAndVerifySuffReplies(looper, wallet1, fClient,
                                        more_reqs_count, 1)
    # Ledger is same for all nodes
    waitNodeDataEquality(looper, alpha, *other_nodes)
    chk(6)  # Since one request is already sent as part of `replied1`
