import json

from plenum.common.constants import TXN_PAYLOAD, TXN_PAYLOAD_DATA
from indy.ledger import multi_sign_request, build_nym_request, sign_and_submit_request
from indy.did import create_and_store_my_did


def test_sigining_without_identifier(looper, txnPoolNodeSet, sdk_pool_handle, sdk_steward_seed, sdk_wallet_handle):
    req = {
        TXN_PAYLOAD: {
            TXN_PAYLOAD_DATA: {
                "aaa": "BBB"
            }
        }
    }

    steward_did_future = create_and_store_my_did(sdk_wallet_handle, json.dumps({"seed": sdk_steward_seed}))
    steward_did, _ = looper.loop.run_until_complete(steward_did_future)

    did_future = create_and_store_my_did(sdk_wallet_handle, json.dumps({}))
    did, verkey = looper.loop.run_until_complete(did_future)

    nym_future = build_nym_request(steward_did, did, verkey, None, None)
    nym = looper.loop.run_until_complete(nym_future)

    resp_future = sign_and_submit_request(sdk_pool_handle, sdk_wallet_handle, steward_did, nym)
    resp = looper.loop.run_until_complete(resp_future)

    req_future = multi_sign_request(sdk_wallet_handle, did, json.dumps(req))
    req = looper.loop.run_until_complete(req_future)
    req = json.loads(req)

    sigs = txnPoolNodeSet[0].init_core_authenticator().authenticate(req)
    assert sigs == [did]
