from zeno.test.client.conftest import passThroughReqAcked1

from zeno.client.client import Client
from zeno.test.malicious_behaviors_client import genDoesntSendRequestToSomeNodes

nodeCount = 4
clientFault = genDoesntSendRequestToSomeNodes("AlphaC")
reqAcked1 = passThroughReqAcked1


def testReplyWhenRequestSentToMoreThanFPlusOneNodes(looper, nodeSet,
                                                    fClient: Client, replied1):
    pass
