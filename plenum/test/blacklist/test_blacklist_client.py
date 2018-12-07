import json

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import sdk_random_request_objects, sdk_send_signed_requests, \
    sdk_get_and_check_replies
from stp_core.loop.eventually import eventually
from plenum.test import waits


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
    req_obj = sdk_random_request_objects(1, identifier=did,
                                         protocol_version=CURRENT_PROTOCOL_VERSION)[0]

    reqs = sdk_send_signed_requests(sdk_pool_handle, [json.dumps(req_obj.as_dict)])

    with pytest.raises(RequestNackedException) as e:
        sdk_get_and_check_replies(looper, reqs)
    assert 'MissingSignature' in e._excinfo[1].args[0]

    def chk():
        for node in txnPoolNodeSet:
            assert not node.isClientBlacklisted(client_name)

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
