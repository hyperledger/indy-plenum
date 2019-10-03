import types

import pytest

from plenum.common.messages.node_messages import Propagate
from stp_core.common.log import getlogger

nodeCount = 4
faultyNodes = 1

logger = getlogger()


# noinspection PyIncorrectDocstring
@pytest.fixture("module")
def node_doesnt_propagate(txnPoolNodeSet):
    """
    Makes the node named Alpha in the given set of nodes faulty.
    After applying this behavior, the node Alpha no longer sends
    propagate requests.
    """

    def evilProcessPropagate(self, msg, frm):
        logger.info("TEST: Evil {} is not processing PROPAGATE".format(self))

    def evilPropagateRequest(self, request, clientName):
        logger.info("TEST: Evil {} is not PROPAGATing client request".
                    format(self))

    # Choosing a node which will not be primary
    node = txnPoolNodeSet[3]
    epp = types.MethodType(evilProcessPropagate, node)
    node.nodeMsgRouter.routes[Propagate] = epp
    node.processPropagate = epp

    node.propagate = types.MethodType(evilPropagateRequest, node)

    return node


# noinspection PyIncorrectDocstring
def testRequestFullRoundTrip(node_doesnt_propagate, replied1):
    """
    With an Alpha that doesn't send propagate requests, the request should
    still be able to successfully complete a full cycle.
    """
    pass
