import json

import pytest

from plenum.common.constants import CURRENT_PROTOCOL_VERSION, DOMAIN_LEDGER_ID
from plenum.common.messages.node_messages import Reply, RequestNack
from plenum.common.request import Request
from plenum.common.txn_util import reqToTxn
from plenum.common.types import f, OPERATION
from plenum.test.helper import sdk_random_request_objects, sdk_sign_request_objects, sdk_multisign_request_object


@pytest.fixture
def node(txnPoolNodeSet):
    return txnPoolNodeSet[0]


def write_request(node, req):
    txns = [reqToTxn(req)]
    node.domainLedger.append_txns_metadata(txns)
    node.domainLedger.appendTxns(txns)
    node.domainLedger.commitTxns(len(txns))
    node.updateSeqNoMap(txns, DOMAIN_LEDGER_ID)


def deserialize_req(req):
    if isinstance(req, str):
        req = json.loads(req)
    if isinstance(req, dict):
        kwargs = dict(
            identifier=req.get(f.IDENTIFIER.nm, None),
            reqId=req.get(f.REQ_ID.nm, None),
            operation=req.get(OPERATION, None),
            signature=req.get(f.SIG.nm, None),
            signatures=req.get(f.SIGS.nm, None),
            protocolVersion=req.get(f.PROTOCOL_VERSION.nm, None)
        )
        req = Request(**kwargs)
    return req


def test_seq_no_db_signed_request(looper, node, sdk_wallet_client):
    # Create signed request and write it to ledger
    req = sdk_random_request_objects(1, identifier=sdk_wallet_client[1], protocol_version=CURRENT_PROTOCOL_VERSION)[0]
    req = sdk_sign_request_objects(looper, sdk_wallet_client, [req])[0]
    req = deserialize_req(req)
    write_request(node, req)

    # Make sure sending request again will return REPLY
    rep = node.getReplyFromLedgerForRequest(req)
    assert isinstance(rep, Reply)


def test_seq_no_db_multisigned_request(looper, node, sdk_wallet_client, sdk_wallet_client2):
    # Create signed request and write it to ledger
    req = sdk_random_request_objects(1, identifier=sdk_wallet_client[1], protocol_version=CURRENT_PROTOCOL_VERSION)[0]
    req = sdk_multisign_request_object(looper, sdk_wallet_client, json.dumps(req.as_dict))
    req = deserialize_req(req)
    write_request(node, req)

    # Make sure sending request again will return REPLY
    rep = node.getReplyFromLedgerForRequest(req)
    assert isinstance(rep, Reply)

    # Make sure sending request with additional signature will return NACK
    multisig_req = sdk_multisign_request_object(looper, sdk_wallet_client2, json.dumps(req.as_dict))
    multisig_req = deserialize_req(multisig_req)
    rep = node.getReplyFromLedgerForRequest(multisig_req)
    assert isinstance(rep, RequestNack)


def test_seq_no_db_unsigned_request(looper, node, sdk_wallet_client):
    # Create unsigned request and write it to ledger
    req = sdk_random_request_objects(1, identifier=sdk_wallet_client[1], protocol_version=CURRENT_PROTOCOL_VERSION)[0]
    write_request(node, req)

    # Make sure sending request again will return REPLY
    rep = node.getReplyFromLedgerForRequest(req)
    assert isinstance(rep, Reply)

    # Make sure sending request with signature will return NACK
    signed_req = sdk_sign_request_objects(looper, sdk_wallet_client, [req])[0]
    signed_req = deserialize_req(signed_req)
    rep = node.getReplyFromLedgerForRequest(signed_req)
    assert isinstance(rep, RequestNack)
