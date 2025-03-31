import json
import time
import pytest

from plenum.test.helper import vdr_get_and_check_replies
from plenum.test.pool_transactions.helper import vdr_sign_and_send_prepared_request

from plenum.common.exceptions import RequestNackedException
from plenum.common.constants import TXN_TYPE, AUDIT, CURRENT_PROTOCOL_VERSION
from plenum.common.types import OPERATION, f


def test_send_audit_txn(looper, vdr_wallet_client, vdr_pool_handle):
    req = {
        OPERATION: {
            TXN_TYPE: AUDIT,
            'data': 'data1'
        },
        f.IDENTIFIER.nm: vdr_wallet_client[1],
        f.REQ_ID.nm: int(time.time()),
        f.PROTOCOL_VERSION.nm: CURRENT_PROTOCOL_VERSION
    }

    rep = vdr_sign_and_send_prepared_request(looper, vdr_wallet_client, vdr_pool_handle, json.dumps(req))
    with pytest.raises(RequestNackedException) as e:
        vdr_get_and_check_replies(looper, [rep])
    e.match('External audit requests are not allowed')
