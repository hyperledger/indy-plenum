from plenum.test.client.conftest import passThroughReqAcked1

from plenum.client.client import Client
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes

nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes(skipCount=3)
reqAcked1 = passThroughReqAcked1


# noinspection PyIncorrectDocstring
def testReplyWhenRequestSentToLessThanFPlusOneNodes(looper, nodeSet,
                                                    fClient: Client, replied1):
    """
    In a system with no faulty nodes, even if the client sends the request to
     one node it will get replies from all the nodes.
    """
