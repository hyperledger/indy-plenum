import json
from plenum.test.wallet_helper import vdr_sign_request
from indy_vdr import ledger
# Look at prepare_txn_author_agreement_acceptance in ledger from vrd. Says to use `Request.set_txn_author_agreement_acceptance` to append to the request

from plenum.common.util import randomString

from plenum.test.pool_transactions.helper import (
    vdr_prepare_nym_request, prepare_new_node_data, vdr_prepare_node_request
)


# TODO makes sense to make more generic and move to upper level helper
def build_nym_request(looper, sdk_wallet):
    return looper.loop.run_until_complete(
        vdr_prepare_nym_request(
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
        vdr_prepare_node_request(steward_did,
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
    req = ledger.prepare_txn_author_agreement_acceptance(text=taa_text,
            version=taa_version,
            taa_digest=None,
            mechanism=taa_acceptance_mech,
            accepted_time=taa_acceptance_time)
    return looper.loop.run_until_complete(req.set_txn_author_agreement_acceptance(
        request_json)
    )


def sign_request_dict(looper, sdk_wallet, req_dict):
    wallet_h, did = sdk_wallet
    req_json = looper.loop.run_until_complete(
        vdr_sign_request(wallet_h, did, json.dumps(req_dict)))
    return json.loads(req_json)
