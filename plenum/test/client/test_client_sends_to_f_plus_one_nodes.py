from plenum.test.client.conftest import passThroughReqAcked1

from plenum.client.client import Client
from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes

nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1


def testReplyWhenRequestSentToMoreThanFPlusOneNodes(looper, nodeSet,
                                                    fClient: Client, replied1):
    pass
