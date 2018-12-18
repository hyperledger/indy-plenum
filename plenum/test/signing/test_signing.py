import pytest
import sys

from plenum.test.delayers import ppDelay, ppgDelay, nom_delay, req_delay
from plenum.test.helper import sdk_json_to_request_object, \
    sdk_send_random_requests
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
def evilAlpha(txnPoolNodeSet):
    makeNodeFaulty(txnPoolNodeSet[0], changesRequest)


faultyNodes = 1


def testOneNodeAltersAClientRequest(looper,
                                    txnPoolNodeSet,
                                    evilAlpha,
                                    sdk_pool_handle,
                                    sdk_wallet_client):
    """Malicious Alpha node sends incorrect propagate. This test check that
    nodes raise InsufficientCorrectSignatures in validate this propagate"""

    alpha = txnPoolNodeSet[0]
    goodNodes = list(txnPoolNodeSet)
    goodNodes.remove(alpha)
    # delay incoming client messages for good nodes by 250 milliseconds
    # this gives Alpha a chance to send a propagate message
    for n in goodNodes:  # type: TestNode
        n.nodeIbStasher.delay(ppDelay(sys.maxsize))
        n.nodeIbStasher.delay(req_delay(sys.maxsize))
    pastNodes = []

    request_couple_json = sdk_send_random_requests(looper, sdk_pool_handle,
                                                   sdk_wallet_client, 1)
    sent1 = sdk_json_to_request_object(request_couple_json[0][0])
    checkPropagated(looper, txnPoolNodeSet, sent1, faultyNodes)

    def check():
        for node in goodNodes:
            if node not in pastNodes:
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
                pastNodes.append(node)

        for node in goodNodes:
            node.nodeIbStasher.resetDelays()



    timeout = waits.expectedClientRequestPropagationTime(len(txnPoolNodeSet))
    looper.run(eventually(check, retryWait=1, timeout=timeout))
