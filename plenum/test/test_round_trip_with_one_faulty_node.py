import types

import pytest

from plenum.common.messages.node_messages import Propagate
from stp_core.common.log import getlogger

nodeCount = 4
faultyNodes = 1

logger = getlogger()


# noinspection PyIncorrectDocstring
@pytest.fixture("module")
def node_doesnt_propagate(startedNodes):
    """
    Makes the node named Alpha in the given set of nodes faulty.
    After applying this behavior, the node Alpha no longer sends
    propagate requests.
    """
    nodes = startedNodes

    def evilProcessPropagate(self, msg, frm):
        logger.info("TEST: Evil {} is not processing PROPAGATE".format(self))

    def evilPropagateRequest(self, request, clientName):
        logger.info("TEST: Evil {} is not PROPAGATing client request".
                    format(self))

    # Choosing a node which will not be primary
    node = nodes.Delta
    epp = types.MethodType(evilProcessPropagate, node)
    node.nodeMsgRouter.routes[Propagate] = epp
    node.processPropagate = epp

    node.propagate = types.MethodType(evilPropagateRequest, node)

    # we don't want `node` being a primary (another test?)
    # nodes.Alpha.delaySelfNomination(100)

    return node


# noinspection PyIncorrectDocstring
def testRequestFullRoundTrip(node_doesnt_propagate, replied1):
    """
    With an Alpha that doesn't send propagate requests, the request should
    still be able to successfully complete a full cycle.
    """
    pass

