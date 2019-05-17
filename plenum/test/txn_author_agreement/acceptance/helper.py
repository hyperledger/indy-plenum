import json
from indy.ledger import (
    append_txn_author_agreement_acceptance_to_request, sign_request
)

from plenum.common.types import f
from plenum.common.request import SafeRequest

from plenum.test.helper import (
    sdk_sign_request_from_dict, sdk_gen_request
)


def gen_signed_request_json(looper, sdk_wallet, operation, taa_acceptance=None):
    wallet_h, did = sdk_wallet
    req = sdk_gen_request(operation, identifier=did)
    req_json = json.dumps(req.as_dict)
    if taa_acceptance is not None:
        req_json = looper.loop.run_until_complete(
            append_txn_author_agreement_acceptance_to_request(
                req_json,
                text=None,
                version=None,
                taa_digest=taa_acceptance[f.TAA_ACCEPTANCE_DIGEST.nm],
                mechanism=taa_acceptance[f.TAA_ACCEPTANCE_MECHANISM.nm],
                time=taa_acceptance[f.TAA_ACCEPTANCE_TIME.nm]
            )
        )
    return looper.loop.run_until_complete(sign_request(wallet_h, did, req_json))


def gen_signed_request_obj(looper, sdk_wallet, operation, taa_acceptance=None):
    req_json = gen_signed_request_json(
        looper, sdk_wallet, operation, taa_acceptance=taa_acceptance
    )
    return SafeRequest(**json.loads(req_json))
