from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Prepare
from stp_core.common.util import adict
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions, whitelistNode
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    sendDuplicate3PhaseMsg
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica
from plenum.test import waits


@pytest.fixture("module")
def setup(txnPoolNodeSet):
    primaryRep, nonPrimaryReps = getPrimaryReplica(txnPoolNodeSet, 0), \
                                 getNonPrimaryReplicas(txnPoolNodeSet, 0)

    # A non primary replica sends duplicate PREPARE requests to all other
    # replicas
    faultyRep = nonPrimaryReps[0]
    makeNodeFaulty(faultyRep.node, partial(sendDuplicate3PhaseMsg,
                                           msgType=Prepare, count=3,
                                           instId=0))

    # The node of the primary replica above should not be blacklisted by any
    # other node since we are simulating multiple PREPARE messages and
    # want to check for a particular suspicion

    whitelistNode(faultyRep.node.name,
                  [node for node in txnPoolNodeSet if node != faultyRep.node],
                  Suspicions.DUPLICATE_PR_SENT.code)

    return adict(primaryRep=primaryRep, nonPrimaryReps=nonPrimaryReps,
                 faultyRep=faultyRep)


# noinspection PyIncorrectDocstring
def testMultiplePrepare(setup, looper, sent1):
    """
    A non primary replica sends multiple PREPARE message to all other
    replicas. Other replicas should raise suspicion for each duplicate
    PREPARE seen and it should count only one PREPARE from that sender
    """

    primaryRep, nonPrimaryReps, faultyRep = setup.primaryRep, \
                                            setup.nonPrimaryReps, setup.faultyRep

    def chkSusp():
        for r in (primaryRep, *nonPrimaryReps):
            if r.name != faultyRep.name:
                # Every node except the one from which duplicate PREPARE was
                # sent should raise suspicion twice, once for each extra
                # PREPARE request

                suspectingNodes = \
                    getNodeSuspicions(r.node,
                                      Suspicions.DUPLICATE_PR_SENT.code)
                assert len(suspectingNodes) == 2

    numOfNodes = len(primaryRep.node.nodeReg)
    timeout = waits.expectedTransactionExecutionTime(numOfNodes)
    looper.run(eventually(chkSusp, retryWait=1, timeout=timeout))
