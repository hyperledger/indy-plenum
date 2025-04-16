import json

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import RequestNackedException
from plenum.test.helper import vdr_random_request_objects, vdr_send_signed_requests, \
    vdr_get_and_check_replies
from stp_core.loop.eventually import eventually
from plenum.test import waits


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
def testDoNotBlacklistClient(looper, txnPoolNodeSet,
                             vdr_wallet_client, vdr_pool_handle,
                             poolTxnClientNames):
    """
    Client should be not be blacklisted by node on sending an unsigned request
    """
    client_name = poolTxnClientNames[0]
    _, did = vdr_wallet_client
    # No node should blacklist the client
    req_obj = vdr_random_request_objects(1, identifier=did,
                                         protocol_version=CURRENT_PROTOCOL_VERSION)[0]

    reqs = vdr_send_signed_requests(vdr_pool_handle, [req_obj], looper)

    with pytest.raises(RequestNackedException, match='MissingSignature'):
        vdr_get_and_check_replies(looper, reqs)

    def chk():
        for node in txnPoolNodeSet:
            assert not node.isClientBlacklisted(client_name)

    timeout = waits.expectedClientToPoolConnectionTimeout(len(txnPoolNodeSet))
    looper.run(eventually(chk, retryWait=1, timeout=timeout))
