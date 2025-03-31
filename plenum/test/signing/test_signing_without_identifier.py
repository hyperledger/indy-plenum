import json

from plenum.common.constants import TXN_PAYLOAD, TXN_PAYLOAD_DATA
from indy_vdr.ledger import build_nym_request
from plenum.test.wallet_helper import vdr_create_and_store_did, vdr_sign_and_submit_request, vdr_multi_sign_request


def test_sigining_without_identifier(looper, txnPoolNodeSet, vdr_pool_handle, vdr_steward_seed, vdr_wallet_handle):
    req = {
        TXN_PAYLOAD: {
            TXN_PAYLOAD_DATA: {
                "aaa": "BBB"
            }
        }
    }

    steward_did_future = vdr_create_and_store_did(vdr_wallet_handle, vdr_steward_seed)
    steward_did, _ = looper.loop.run_until_complete(steward_did_future)

    did_future = vdr_create_and_store_did(vdr_wallet_handle)
    did, verkey = looper.loop.run_until_complete(did_future)

    nym = build_nym_request(steward_did, did, verkey, None, None)

    resp_future = vdr_sign_and_submit_request(vdr_pool_handle, vdr_wallet_handle, steward_did, nym)
    resp = looper.loop.run_until_complete(resp_future)

    req_future = vdr_multi_sign_request(vdr_wallet_handle, did, json.dumps(req))
    req = looper.loop.run_until_complete(req_future)
    req = json.loads(req)

    sigs = txnPoolNodeSet[0].init_core_authenticator().authenticate(req)
    assert sigs == [did]
