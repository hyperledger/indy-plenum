import json

import pytest

from random import randint

from plenum.common.constants import DATA
from plenum.common.exceptions import RequestNackedException
from plenum.common.txn_util import get_seq_no
from plenum.test.pool_transactions.helper import \
    vdr_sign_and_send_prepared_request, vdr_prepare_nym_request, \
    vdr_build_get_txn_request
from stp_core.loop.eventually import eventually
from plenum.test.helper import vdr_get_and_check_replies
from plenum.common.util import getMaxFailures, randomString

c_delay = 10
fValue = getMaxFailures(4)


INVALID_LEDGER_ID = 5908
INVALID_SEQ_NO = -23
whitelist = ["GET_TXN has no seq_no, skip AuditProof logic", "Given signature is not for current root hash, aborting"]


def test_get_txn_for_invalid_ledger_id(looper, txnPoolNodeSet,
                                       vdr_wallet_steward,
                                       vdr_pool_handle):
    _, steward_did = vdr_wallet_steward
    request = vdr_build_get_txn_request(looper, steward_did, 1)

    # setting incorrect Ledger_ID
    request_json = json.loads(request)
    request_json['operation']['ledgerId'] = INVALID_LEDGER_ID
    request = json.dumps(request_json)

    request_couple = \
        vdr_sign_and_send_prepared_request(looper,
                                           vdr_wallet_steward,
                                           vdr_pool_handle,
                                           request)
    with pytest.raises(RequestNackedException) as e:
        vdr_get_and_check_replies(looper, [request_couple])
    assert 'expected one of' in e._excinfo[1].args[0]


def test_get_txn_for_invalid_seq_no(looper, txnPoolNodeSet,
                                    vdr_wallet_steward,
                                    vdr_pool_handle):
    _, steward_did = vdr_wallet_steward

    # setting incorrect data
    request = vdr_build_get_txn_request(looper, steward_did,
                                        INVALID_SEQ_NO)

    request_couple = \
        vdr_sign_and_send_prepared_request(looper,
                                           vdr_wallet_steward,
                                           vdr_pool_handle,
                                           request)
    with pytest.raises(RequestNackedException) as e:
        vdr_get_and_check_replies(looper, [request_couple])
    assert 'cannot be smaller' in e._excinfo[1].args[0]


def test_get_txn_for_existing_seq_no(looper, txnPoolNodeSet,
                                     vdr_wallet_steward,
                                     vdr_pool_handle):
    _, steward_did = vdr_wallet_steward
    for i in range(2):
        request = vdr_build_get_txn_request(looper, steward_did, 1)

        # Check with and without ledger id
        request_json = json.loads(request)
        if i:
            request_json['operation']['ledgerId'] = 1
        request = json.dumps(request_json)

        vdr_sign_and_send_prepared_request(looper,
                                           vdr_wallet_steward,
                                           vdr_pool_handle,
                                           request)


def test_get_txn_for_non_existing_seq_no(looper, txnPoolNodeSet,
                                         vdr_wallet_steward,
                                         vdr_pool_handle):
    _, steward_did = vdr_wallet_steward

    # setting incorrect data
    def generate_non_existing_seq_no():
        return randint(500, 1000)

    request = vdr_build_get_txn_request(looper, steward_did,
                                        generate_non_existing_seq_no())

    request_couple = \
        vdr_sign_and_send_prepared_request(looper,
                                           vdr_wallet_steward,
                                           vdr_pool_handle,
                                           request)
    reply = vdr_get_and_check_replies(looper, [request_couple])[0][1]
    assert reply['result'][DATA] is None


def test_get_txn_response_as_expected(looper, txnPoolNodeSet,
                                      vdr_pool_handle,
                                      vdr_wallet_steward):
    seed = randomString(32)
    wh, _ = vdr_wallet_steward

    # filling nym request and getting steward did
    # if role == None, we are adding client
    nym_request, new_did = looper.loop.run_until_complete(
        vdr_prepare_nym_request(vdr_wallet_steward, seed,
                            None, None))

    # sending request using 'sdk_' functions
    request_couple = vdr_sign_and_send_prepared_request(
        looper, vdr_wallet_steward,
        vdr_pool_handle, nym_request)

    result1 = vdr_get_and_check_replies(looper,
                                        [request_couple])[0][1]['result']
    seqNo = get_seq_no(result1)

    _, steward_did = vdr_wallet_steward
    request = vdr_build_get_txn_request(looper, steward_did, seqNo)

    request_couple = \
        vdr_sign_and_send_prepared_request(looper,
                                           vdr_wallet_steward,
                                           vdr_pool_handle,
                                           request)
    result2 = vdr_get_and_check_replies(looper,
                                        [request_couple])[0][1]['result']

    assert result1['reqSignature'] == result2['data']['reqSignature']
    assert result1['txn'] == result2['data']['txn']
    assert result1['txnMetadata'] == result2['data']['txnMetadata']
    assert result1['rootHash'] == result2['data']['rootHash']
    assert result1['ver'] == result2['data']['ver']
    assert result1['auditPath'] == result2['data']['auditPath']
