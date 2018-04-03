import json

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_random_request_objects, sdk_send_signed_requests, sdk_send_random_request, \
    sdk_sign_request_objects, sdk_get_and_check_replies
from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.test.malicious_behaviors_client import makeClientFaulty, \
    sendsUnsignedRequest


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
def testDoNotBlacklistClient(looper, txnPoolNodeSet,
                             sdk_wallet_client, sdk_pool_handle,
                             poolTxnClientNames):
    """
    Client should be not be blacklisted by node on sending an unsigned request
    """
    client_name = poolTxnClientNames[0]
    _, did = sdk_wallet_client
    # No node should blacklist the client
    reqs_obj = sdk_random_request_objects(1, identifier=did,
                                          protocol_version=CURRENT_PROTOCOL_VERSION)
    req = sdk_sign_request_objects(looper, sdk_wallet_client, reqs_obj)[0]

    # break the signature
    request_json = json.loads(req)
    request_json['reqId'] = request_json['reqId'] + 1
    req = json.dumps(request_json)

    reqs = sdk_send_signed_requests(sdk_pool_handle, [req])

    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, reqs)
    assert 'InsufficientCorrectSignatures' in e._excinfo[1].args[0]

    def chk():
        for node in txnPoolNodeSet:
            assert not node.isClientBlacklisted(client_name)

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
