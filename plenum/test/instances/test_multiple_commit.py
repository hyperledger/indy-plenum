import types
from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import Commit
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

    faultyRep = nonPrimaryReps[0]
    makeNodeFaulty(faultyRep.node, partial(sendDuplicate3PhaseMsg,
                                           msgType=Commit, count=3,
                                           instId=0))

    # The node of the primary replica above should not be blacklisted by any
    # other node since we are simulating multiple COMMIT messages and
    # want to check for a particular suspicion

    whitelistNode(faultyRep.node.name,
                  [node for node in txnPoolNodeSet if node != faultyRep.node],
                  Suspicions.DUPLICATE_CM_SENT.code)

    # If the request is ordered then COMMIT will be rejected much earlier
    for r in [primaryRep, *nonPrimaryReps]:
        def do_nothing(self, commit):
            pass

        r.doOrder = types.MethodType(do_nothing, r)

    return adict(primaryRep=primaryRep, nonPrimaryReps=nonPrimaryReps,
                 faultyRep=faultyRep)


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
def testMultipleCommit(setup, looper, sent1):
    """
    A replica sends multiple COMMIT messages to all other replicas. Other
    replicas should raise suspicion for each duplicate COMMIT seen and it
    should count only one COMMIT from that sender
    """
    primaryRep, nonPrimaryReps, faultyRep = setup.primaryRep, \
                                            setup.nonPrimaryReps, setup.faultyRep

    def chkSusp():
        for r in (primaryRep, *nonPrimaryReps):
            if r.name != faultyRep.name:
                # Every node except the one from which duplicate COMMIT was
                # sent should raise suspicion twice, once for each extra
                # PREPARE request
                assert len(
                    getNodeSuspicions(
                        r.node,
                        Suspicions.DUPLICATE_CM_SENT.code)) == 2

    numOfNodes = len(primaryRep.node.nodeReg)
    timeout = waits.expectedTransactionExecutionTime(numOfNodes)
    looper.run(eventually(chkSusp, retryWait=1, timeout=timeout))
