import pytest

from plenum.common.exceptions import InvalidSignature
from plenum.test.client.conftest import passThroughReqAcked1
from plenum.test.helper import whitelistNode
from plenum.test.malicious_behaviors_node import makeNodeFaulty, changesRequest

from plenum.test.malicious_behaviors_client import \
    genDoesntSendRequestToSomeNodes

nodeCount = 4
faultyNodes = 1
clientFault = genDoesntSendRequestToSomeNodes("GammaC", "DeltaC")
reqAcked1 = passThroughReqAcked1

whitelist = ['for InvalidSignature', 'discarding message']


@pytest.fixture(scope="module")
def nodeChangesRequest(nodeSet):
    alpha = nodeSet.Alpha

    # Alpha should not be blacklisted for Invalid Signature by all other nodes
    whitelistNode(alpha.name,
                  [node for node in nodeSet if node != alpha],
                  InvalidSignature.code)
    makeNodeFaulty(alpha, changesRequest,)


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
def testReplyUnaffectedByFaultyNode(looper, nodeSet, nodeChangesRequest,
                                    fClient, replied1):
    """
    Client is malicious - sends requests to Alpha and Beta only
    Node Alpha is malicious - it alters the request
    There will be two requests with two different OP values and the same reqId.
    But the modified one will be dropped as it fails signature verification in
    the PROPAGATE step. A REPLY will be generated for the correct request and
    send to client.
    """
