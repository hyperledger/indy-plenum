import pytest
import sys

from plenum.test.delayers import ppDelay, ppgDelay
from plenum.test.helper import sdk_json_to_request_object
from stp_core.loop.eventually import eventually
from plenum.common.exceptions import InsufficientCorrectSignatures
from stp_core.common.log import getlogger
from stp_core.common.util import adict
from plenum.test import waits
from plenum.test.malicious_behaviors_node import changesRequest, makeNodeFaulty, \
    delaysPrePrepareProcessing
from plenum.test.node_request.node_request_helper import checkPropagated
from plenum.test.test_node import TestNode

logger = getlogger()
whitelist = ['doing nothing for now',
             'InvalidSignature']


@pytest.fixture(scope="module")
def setup(txnPoolNodeSet):
    alpha = txnPoolNodeSet[0]
    alpha.nodeIbStasher.delay(ppgDelay(sys.maxsize))
    pool_without_alpha = list(txnPoolNodeSet)
    pool_without_alpha.remove(alpha)
    return adict(goodNodes=pool_without_alpha)


@pytest.fixture(scope="module")
def evilAlpha(txnPoolNodeSet):
    makeNodeFaulty(txnPoolNodeSet[0], changesRequest)


faultyNodes = 1


def testOneNodeAltersAClientRequest(looper,
                                    txnPoolNodeSet,
                                    setup,
                                    evilAlpha,
                                    sent1):
    sent1 = sdk_json_to_request_object(sent1[0][0])
    checkPropagated(looper, txnPoolNodeSet, sent1, faultyNodes)

    goodNodes = setup.goodNodes

    def check():
        for node in goodNodes:

            # ensure the nodes are suspicious of Alpha
            params = node.spylog.getLastParams(TestNode.reportSuspiciousNode)
            frm = params["nodeName"]
            reason = params["reason"]
            assert frm == 'Alpha'
            assert reason == InsufficientCorrectSignatures.reason.format(0, 1)

            # ensure Alpha's propagates were ignored by the other nodes
            key = sent1.digest
            props = node.requests[key].propagates
            assert 'Alpha' not in props
            for good in goodNodes:
                assert good.name in props



    timeout = waits.expectedClientRequestPropagationTime(len(txnPoolNodeSet))
    looper.run(eventually(check, retryWait=1, timeout=timeout))
