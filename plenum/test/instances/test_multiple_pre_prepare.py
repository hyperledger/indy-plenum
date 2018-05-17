from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.util import adict
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test.instances.helper import sentPrepare
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    sendDuplicate3PhaseMsg
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica
from plenum.test import waits


@pytest.fixture("module")
def setup(txnPoolNodeSet):
    primaryRep, nonPrimaryReps = getPrimaryReplica(txnPoolNodeSet, 0), \
                                 getNonPrimaryReplicas(txnPoolNodeSet, 0)

    # The primary replica would send 3 duplicate PRE-PREPARE requests to
    # non primary replicas
    makeNodeFaulty(primaryRep.node, partial(sendDuplicate3PhaseMsg,
                                            msgType=PrePrepare, count=3))

    # The node of the primary replica above should not be blacklisted by any
    # other node since we are simulating multiple PRE-PREPARE messages and
    # want to check for a particular suspicion

    return adict(primaryRep=primaryRep, nonPrimaryReps=nonPrimaryReps)


# noinspection PyIncorrectDocstring
def testMultiplePrePrepareWithSameSeqNo(setup, looper, sent1):
    """
    A primary replica sends duplicate PRE-PREPARE messages to the non primary
    replicas but non primary replicas should raise suspicion on encountering
    each duplicate PRE-PREPARE. Also it should send only one PREPARE
    """

    primaryRep, nonPrimaryReps = setup.primaryRep, setup.nonPrimaryReps

    def chkSusp():
        for r in nonPrimaryReps:
            # Every node with non primary replicas of instance 0 should raise
            # suspicion twice, once for each extra PRE-PREPARE request

            suspectingNodes = \
                getNodeSuspicions(r.node,
                                  Suspicions.DUPLICATE_PPR_SENT.code)
            assert len(suspectingNodes) == 2

            # Each non primary replica should just send one PREPARE
            assert len(sentPrepare(r)) == 1

    numOfNodes = len(primaryRep.node.nodeReg)
    timeout = waits.expectedTransactionExecutionTime(numOfNodes)
    looper.run(eventually(chkSusp, retryWait=1, timeout=timeout))
