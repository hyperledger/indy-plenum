import json
from indy.ledger import (
    append_txn_author_agreement_acceptance_to_request, sign_request
)

from plenum.common.util import randomString

from plenum.test.pool_transactions.helper import (
    prepare_nym_request, prepare_new_node_data, prepare_node_request
)


# TODO makes sense to make more generic and move to upper level helper
def build_nym_request(looper, sdk_wallet):
    return looper.loop.run_until_complete(
        prepare_nym_request(
            sdk_wallet,
            named_seed=randomString(32),
            alias=randomString(5),
            role=None
        )
    )[0]


# TODO makes sense to make more generic and move to upper level helper
def build_node_request(looper, tconf, tdir, sdk_wallet):
    new_node_name = 'Node' + randomString(3)
    sigseed, verkey, bls_key, nodeIp, nodePort, clientIp, clientPort, key_proof = \
        prepare_new_node_data(tconf, tdir, new_node_name)

    _, steward_did = sdk_wallet
    node_request = looper.loop.run_until_complete(
        prepare_node_request(steward_did,
                             new_node_name=new_node_name,
                             clientIp=clientIp,
                             clientPort=clientPort,
                             nodeIp=nodeIp,
                             nodePort=nodePort,
                             bls_key=bls_key,
                             sigseed=sigseed,
                             services=[],
                             key_proof=key_proof))
    return node_request


def add_taa_acceptance(
    looper,
    request_json,
    taa_text,
    taa_version,
    taa_acceptance_mech,
    taa_acceptance_time
):
    return looper.loop.run_until_complete(
        append_txn_author_agreement_acceptance_to_request(
            request_json,
            text=taa_text,
            version=taa_version,
            taa_digest=None,
            mechanism=taa_acceptance_mech,
            time=taa_acceptance_time
        )
    )


def sign_request_dict(looper, sdk_wallet, req_dict):
    wallet_h, did = sdk_wallet
    req_json = looper.loop.run_until_complete(
        sign_request(wallet_h, did, json.dumps(req_dict)))
    return json.loads(req_json)
