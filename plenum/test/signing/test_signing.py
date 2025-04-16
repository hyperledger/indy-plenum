import pytest
import sys
import json

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.test.delayers import ppDelay, req_delay
from plenum.test.helper import vdr_send_random_requests, vdr_get_replies, sdk_random_request_objects, sdk_multisign_request_object, sdk_send_signed_requests, sdk_get_and_check_replies
from plenum.test.node_request.test_reply_from_ledger_for_request import deserialize_req
from stp_core.loop.eventually import eventually
from plenum.common.exceptions import InsufficientCorrectSignatures, RequestNackedException
from stp_core.common.log import getlogger
from plenum.test import waits
from plenum.test.malicious_behaviors_node import changesRequest, makeNodeFaulty
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
                                    vdr_pool_handle,
                                    vdr_wallet_client):
    """Malicious Alpha node sends incorrect propagate. This test check that
    nodes raise InsufficientCorrectSignatures in validate this propagate"""

    # TODO: This test is throwing a `indy.error.PoolLedgerTerminated` exception
    #  This is probably happening because a request is sent and the pool is terminated before the reply is processed

    alpha = txnPoolNodeSet[0]
    goodNodes = list(txnPoolNodeSet)
    goodNodes.remove(alpha)
    # delay incoming client messages for good nodes by 250 milliseconds
    # this gives Alpha a chance to send a propagate message
    
    # The previous delay was sent to 8 and a half years not 0.25 seconds
    for n in goodNodes:  # type: TestNode
        n.nodeIbStasher.delay(ppDelay(0.250))
        n.nodeIbStasher.delay(req_delay(0.250))
    pastNodes = []

    sent1 = vdr_send_random_requests(looper, vdr_pool_handle, vdr_wallet_client, 1)
    sent1 = vdr_get_replies(looper, sent1)
    #sent1 = sdk_json_to_request_object(request_couple_json[0][0])
    checkPropagated(looper, txnPoolNodeSet, sent1, faultyNodes)

    def check():
        for node in goodNodes:
            if node not in pastNodes:
                # ensure the nodes are suspicious of Alpha
                params = node.spylog.getLastParams(TestNode.reportSuspiciousNode)
                frm = params["nodeName"]
                reason = params["reason"]
                assert frm == 'Alpha'
                invalid_signatures = 'did={}, signature={}'.format(sent1[0][0]["identifier"], sent1[0][0]["signature"])
                assert reason == InsufficientCorrectSignatures.reason.format(1, 0, 1, invalid_signatures)

                # ensure Alpha's propagates were ignored by the other nodes
                key = json.loads(sent1[0][1][node.name])["result"]["txn"]["metadata"]["digest"]
                props = node.requests[key].propagates
                assert 'Alpha' not in props
                for good in goodNodes:
                    assert good.name in props
                pastNodes.append(node)

        for node in goodNodes:
            node.nodeIbStasher.resetDelays()

    timeout = waits.expectedClientRequestPropagationTime(len(txnPoolNodeSet))
    looper.run(eventually(check, retryWait=1, timeout=timeout))

#indy-sdk request manipulation test (invalid in vdr)
def test_request_with_incorrect_multisig_signatures(looper, sdk_pool_handle, sdk_wallet_client, sdk_wallet_client2):
    req = sdk_random_request_objects(1, identifier=sdk_wallet_client[1], protocol_version=CURRENT_PROTOCOL_VERSION)[0]

    req = sdk_multisign_request_object(looper, sdk_wallet_client, json.dumps(req.as_dict))
    req = deserialize_req(req)
    req.signatures[req.identifier] = 'garbage'

    multisig_req = sdk_multisign_request_object(looper, sdk_wallet_client2, json.dumps(req.as_dict))

    rep1 = sdk_send_signed_requests(looper, sdk_pool_handle, [multisig_req])

    invalid_signatures = 'did={}, signature={}'.format(req.identifier, req.signatures[req.identifier])
    expected_error_message = 'Reason: client request invalid: {}'.\
        format(InsufficientCorrectSignatures.reason.format(2, 1, 1, invalid_signatures))

    with pytest.raises(RequestNackedException, match=expected_error_message):
        sdk_get_and_check_replies(looper, rep1)
