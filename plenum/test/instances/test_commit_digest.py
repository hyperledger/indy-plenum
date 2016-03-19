from functools import partial

import pytest
from plenum.test.eventually import eventually
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    send3PhaseMsgWithIncorrectDigest
from plenum.common.util import adict

from plenum.common.request_types import Commit
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getPrimaryReplica, getNonPrimaryReplicas, \
    getNodeSuspicions

whitelist = [Suspicions.CM_DIGEST_WRONG.reason,
             'cannot process incoming COMMIT']


@pytest.fixture("module")
def setup(nodeSet, up):
    primaryRep = getPrimaryReplica(nodeSet, 0)
    nonPrimaryReps = getNonPrimaryReplicas(nodeSet, 0)

    faultyRep = nonPrimaryReps[0]
    makeNodeFaulty(faultyRep.node, partial(send3PhaseMsgWithIncorrectDigest,
                                           msgType=Commit, instId=0))

    return adict(primaryRep=primaryRep, nonPrimaryReps=nonPrimaryReps,
                 faultyRep=faultyRep)


# noinspection PyIncorrectDocstring
def testCommitDigest(setup, looper, sent1):
    """
    A replica COMMIT messages with incorrect digests to all other replicas.
    Other replicas should raise suspicion for the COMMIT seen
    """
    primaryRep = setup.primaryRep
    nonPrimaryReps = setup.nonPrimaryReps
    faultyRep = setup.faultyRep

    def chkSusp():
        for r in (primaryRep, *nonPrimaryReps):
            if r.name != faultyRep.name:
                # Every node except the one from which COMMIT with incorrect
                # digest was sent should raise suspicion for the COMMIT
                # message
                susps = getNodeSuspicions(r.node,
                                          Suspicions.CM_DIGEST_WRONG.code)
                assert len(susps) == 1

    looper.run(eventually(chkSusp, retryWait=1, timeout=20))
