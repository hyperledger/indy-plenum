from functools import partial

import pytest
from zeno.test.eventually import eventually
from zeno.test.helper import getPrimaryReplica, getNonPrimaryReplicas, \
    getNodeSuspicions
from zeno.test.malicious_behaviors_node import makeNodeFaulty, \
    send3PhaseMsgWithIncorrectDigest
from zeno.test.testing_utils import adict

from zeno.common.request_types import PrePrepare
from zeno.server.suspicion_codes import Suspicions
from zeno.test.instances.helper import sentPrepare

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
