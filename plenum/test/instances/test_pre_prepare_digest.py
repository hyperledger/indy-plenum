from functools import partial

import pytest

from stp_core.loop.eventually import eventually
from plenum.common.messages.node_messages import PrePrepare
from stp_core.common.util import adict
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import getNodeSuspicions
from plenum.test.instances.helper import sentPrepare
from plenum.test.malicious_behaviors_node import makeNodeFaulty, \
    send3PhaseMsgWithIncorrectDigest
from plenum.test.test_node import getNonPrimaryReplicas, getPrimaryReplica
from plenum.test import waits


@pytest.fixture("module")
def setup(txnPoolNodeSet):
    primaryRep, nonPrimaryReps = getPrimaryReplica(txnPoolNodeSet, 0), \
                                 getNonPrimaryReplicas(txnPoolNodeSet, 0)

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
            susp_code = Suspicions.PPR_DIGEST_WRONG.code
            # Since the node sending bad requests might become primary of
            # some backup instance after view changes, it will again send a
            # PRE-PREPARE with incorrect digest, so other nodes might raise
            # suspicion more than once
            assert len(getNodeSuspicions(r.node,
                                         susp_code)) >= 1
            # No non primary replica should send any PREPARE
            assert len(sentPrepare(r, viewNo=0, ppSeqNo=1)) == 0

    numOfNodes = len(primaryRep.node.nodeReg)
    timeout = waits.expectedTransactionExecutionTime(numOfNodes)
    looper.run(eventually(chkSusp, retryWait=1, timeout=timeout))
