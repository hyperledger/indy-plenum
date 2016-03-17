import logging
import types

import pytest

from plenum.common.request_types import Propagate

nodeCount = 4
faultyNodes = 1


# noinspection PyIncorrectDocstring
@pytest.fixture("module")
def alphaDoesntPropagate(startedNodes):
    """
    Makes the node named Alpha in the given set of nodes faulty.
    After applying this behavior, the node Alpha no longer sends
    propagate requests.
    """
    nodes = startedNodes
    async def evilProcessPropagate(self, msg, frm):
        logging.info("TEST: Evil {} is not processing PROPAGATE".format(self))

    def evilPropagateRequest(self, request, clientName):
        logging.info("TEST: Evil {} is not PROPAGATing client request".format(self))

    epp = types.MethodType(evilProcessPropagate, nodes.Alpha)
    nodes.Alpha.nodeMsgRouter.routes[Propagate] = epp
    nodes.Alpha.processPropagate = epp

    nodes.Alpha.propagate = types.MethodType(evilPropagateRequest, nodes.Alpha)

    # we don't want Alpha having a primary (another test?)
    nodes.Alpha.delaySelfNomination(100)

    return nodes.Alpha


# noinspection PyIncorrectDocstring
def testRequestFullRoundTrip(alphaDoesntPropagate, replied1):
    """
    With an Alpha that doesn't send propagate requests, the request should
    still be able to successfully complete a full cycle.
    """
    pass
