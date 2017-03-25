from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.types import PrePrepare
from plenum.common.util import adict
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getPrimaryReplica, getNodeSuspicions
from plenum.test.instances.helper import sentPrepare
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    send3PhaseMsgWithIncorrectDigest
from plenum.test.test_node import getNonPrimaryReplicas

whitelist = [Suspicions.PPR_DIGEST_WRONG.reason,
             'cannot process incoming PRE-PREPARE']


@pytest.fixture("module")
def setup(nodeSet, up):
    primaryRep, nonPrimaryReps = getPrimaryReplica(nodeSet, 0), \
                                 getNonPrimaryReplicas(nodeSet, 0)

    # The primary replica would send PRE-PREPARE messages with incorrect digest
    makeNodeFaulty(primaryRep.node, partial(send3PhaseMsgWithIncorrectDigest,
                                            msgType=PrePrepare))

    return adict(primaryRep=primaryRep, nonPrimaryReps=nonPrimaryReps)


# noinspection PyIncorrectDocstring
def testPrePrepareDigest(setup, looper, sent1):
    """
    A primary replica sends PRE-PREPARE message with incorrect digest to the
    non primary replicas but non primary replicas should raise suspicion on
    encountering the PRE-PREPARE. Also it should send no PREPARE
    """
    primaryRep, nonPrimaryReps = setup.primaryRep, setup.nonPrimaryReps

    def chkSusp():
        for r in nonPrimaryReps:
            # Every node with non primary replicas of instance 0 should raise
            # suspicion
            assert len(getNodeSuspicions(r.node,
                                         Suspicions.PPR_DIGEST_WRONG.code)) == 1
            # No non primary replica should send any PREPARE
            assert len(sentPrepare(r)) == 0

    looper.run(eventually(chkSusp, retryWait=1, timeout=20))
