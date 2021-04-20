import json

from indy.ledger import build_ledgers_freeze_request, build_get_frozen_ledgers_request
from plenum.test.helper import sdk_get_and_check_replies, \
    sdk_send_signed_requests, sdk_sign_and_submit_req_obj, sdk_multi_sign_request_objects, sdk_json_to_request_object


def sdk_send_freeze_ledgers(looper, sdk_pool_handle, sdk_wallets, ledgers_ids: [int]):
    req = looper.loop.run_until_complete(build_ledgers_freeze_request(sdk_wallets[0][1], ledgers_ids))
    signed_reqs = sdk_multi_sign_request_objects(looper, sdk_wallets,
                                                 [sdk_json_to_request_object(json.loads(req))])
    reps = sdk_send_signed_requests(sdk_pool_handle, signed_reqs)
    return sdk_get_and_check_replies(looper, reps)[0]


def sdk_get_frozen_ledgers(looper, sdk_pool_handle, sdk_wallet):
    req = looper.loop.run_until_complete(build_get_frozen_ledgers_request(sdk_wallet[1]))
    rep = sdk_sign_and_submit_req_obj(looper, sdk_pool_handle, sdk_wallet, sdk_json_to_request_object(json.loads(req)))
    return sdk_get_and_check_replies(looper, [rep])[0]
