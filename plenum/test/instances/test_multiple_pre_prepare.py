from functools import partial

import pytest

from plenum.test.eventually import eventually
from plenum.test.helper import getPrimaryReplica, getNodeSuspicions, \
    getNonPrimaryReplicas
from plenum.test.malicious_behaviors_node import makeNodeFaulty, sendDuplicate3PhaseMsg
from plenum.common.util import adict

from plenum.common.types import PrePrepare
from plenum.server.suspicion_codes import Suspicions
from plenum.test.instances.helper import sentPrepare

whitelist = [Suspicions.DUPLICATE_PPR_SENT.reason,
             'cannot process incoming PRE-PREPARE',
             Suspicions.UNKNOWN_PR_SENT.reason,
             'Invalid prepare message received',
             'cannot process incoming PREPARE',
             Suspicions.UNKNOWN_CM_SENT.reason,
             'cannot process incoming COMMIT']

@pytest.fixture("module")
def setup(nodeSet, up):
    primaryRep, nonPrimaryReps = getPrimaryReplica(nodeSet, 0), \
                                 getNonPrimaryReplicas(nodeSet, 0)

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
            assert len(getNodeSuspicions(r.node,
                                         Suspicions.DUPLICATE_PPR_SENT.code)) == 2
            # Each non primary replica should just send one PREPARE
            assert len(sentPrepare(r)) == 1

    looper.run(eventually(chkSusp, retryWait=1, timeout=20))
