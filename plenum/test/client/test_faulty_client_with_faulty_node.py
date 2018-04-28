import pytest

from plenum.common.exceptions import InvalidSignature
from plenum.test.client.conftest import passThroughReqAcked1
from plenum.test.helper import whitelistNode
from plenum.test.malicious_behaviors_node import makeNodeFaulty, changesRequest

nodeCount = 4
faultyNodes = 1
reqAcked1 = passThroughReqAcked1

whitelist = ['for InvalidSignature', 'discarding message']


@pytest.fixture(scope="module")
def txnPoolNodeSet(txnPoolNodeSet):
    delta = txnPoolNodeSet[-1]
    assert not delta.master_replica.isPrimary

    # Alpha should not be blacklisted for Invalid Signature by all other nodes
    whitelistNode(delta.name,
                  [node for node in txnPoolNodeSet if node != delta],
                  InvalidSignature.code)
    makeNodeFaulty(delta, changesRequest, )
    txnPoolNodeSet[2].clientstack.stop()
    return txnPoolNodeSet


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
def testReplyUnaffectedByFaultyNode(looper,
                                    replied1):
    """
    Client is malicious - sends requests to Alpha and Beta only
    Node Alpha is malicious - it alters the request
    There will be two requests with two different OP values and the same reqId.
    But the modified one will be dropped as it fails signature verification in
    the PROPAGATE step. A REPLY will be generated for the correct request and
    send to client.
    """
