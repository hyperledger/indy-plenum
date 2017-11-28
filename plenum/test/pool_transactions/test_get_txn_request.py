from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CLIENT_BLACKLISTER_SUFFIX, \
    NODE_BLACKLISTER_SUFFIX, NODE_PRIMARY_STORAGE_SUFFIX, HS_FILE, HS_LEVELDB, TXN_TYPE, LedgerState, LEDGER_STATUS, \
    CLIENT_STACK_SUFFIX, PRIMARY_SELECTION_PREFIX, VIEW_CHANGE_PREFIX, OP_FIELD_NAME, CATCH_UP_PREFIX, NYM, \
    POOL_TXN_TYPES, GET_TXN, DATA, MONITORING_PREFIX, TXN_TIME, VERKEY, TARGET_NYM, ROLE, STEWARD, TRUSTEE, ALIAS, \
    NODE_IP, CURRENT_PROTOCOL_VERSION
from random import randint

from plenum.common.types import f
from plenum.test.pool_transactions.helper import sendAddNewClient
from stp_core.loop.eventually import eventually
from plenum.test import waits
from plenum.test.helper import check_sufficient_replies_received, \
    waitReqNackWithReason
from plenum.common.util import getMaxFailures
from plenum.common.request import Request

c_delay = 10
fValue = getMaxFailures(4)


def test_get_txn_for_invalid_ledger_id(looper, txnPoolNodeSet,
                                       steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        f.LEDGER_ID.nm: 5908,
        DATA: 1
    }
    req = Request(identifier=stewardWallet.defaultId,
                  operation=op, reqId=Request.gen_req_id(),
                  protocolVersion=CURRENT_PROTOCOL_VERSION)
    steward1.submitReqs(req)
    for node in txnPoolNodeSet:
        waitReqNackWithReason(looper, steward1, 'expected one of',
                              node.clientstack.name)


def test_get_txn_for_invalid_seq_no(looper, txnPoolNodeSet,
                                    steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
        DATA: -23
    }
    req = Request(identifier=stewardWallet.defaultId,
                  operation=op, reqId=Request.gen_req_id(),
                  protocolVersion=CURRENT_PROTOCOL_VERSION)
    steward1.submitReqs(req)
    for node in txnPoolNodeSet:
        waitReqNackWithReason(looper, steward1, 'cannot be smaller',
                              node.clientstack.name)


def test_get_txn_for_existing_seq_no(looper, steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        DATA: 1
    }

    def chk():
        nonlocal op
        req = Request(identifier=stewardWallet.defaultId,
                      operation=op, reqId=Request.gen_req_id(),
                      protocolVersion=CURRENT_PROTOCOL_VERSION)
        steward1.submitReqs(req)

        timeout = waits.expectedTransactionExecutionTime(
            len(steward1.inBox)) + c_delay
        get_txn_response = \
            looper.run(eventually(check_sufficient_replies_received,
                                  steward1, req.identifier, req.reqId,
                                  retryWait=1, timeout=timeout))

        assert get_txn_response[DATA]

    # Check with and without ledger id
    chk()
    op[f.LEDGER_ID.nm] = DOMAIN_LEDGER_ID
    chk()


def test_get_txn_for_non_existing_seq_no(looper, steward1, stewardWallet):
    op = {
        TXN_TYPE: GET_TXN,
        f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
        DATA: randint(100, 1000)
    }
    req = Request(identifier=stewardWallet.defaultId,
                  operation=op, reqId=Request.gen_req_id(),
                  protocolVersion=CURRENT_PROTOCOL_VERSION)
    steward1.submitReqs(req)

    timeout = waits.expectedTransactionExecutionTime(
        len(steward1.inBox)) + c_delay
    get_txn_response = \
        looper.run(eventually(check_sufficient_replies_received,
                              steward1, req.identifier, req.reqId,
                              retryWait=1, timeout=timeout))

    assert not get_txn_response[DATA]


def test_get_txn_response_as_expected(looper, steward1, stewardWallet):
    req, wallet = sendAddNewClient(STEWARD, "name", steward1, stewardWallet)

    timeout = waits.expectedTransactionExecutionTime(
        len(steward1.inBox)) + c_delay
    nym_response = \
        looper.run(eventually(check_sufficient_replies_received,
                              steward1, req.identifier, req.reqId,
                              retryWait=1, timeout=timeout))
    op = {
        TXN_TYPE: GET_TXN,
        f.LEDGER_ID.nm: DOMAIN_LEDGER_ID,
        DATA: nym_response['seqNo']
    }
    req = Request(identifier=stewardWallet.defaultId,
                  operation=op, reqId=Request.gen_req_id(),
                  protocolVersion=CURRENT_PROTOCOL_VERSION)
    steward1.submitReqs(req)

    get_txn_response = \
        looper.run(eventually(check_sufficient_replies_received,
                              steward1, req.identifier, req.reqId,
                              retryWait=1, timeout=timeout))

    nym_response.pop('txnTime', None)
    get_txn_response[DATA].pop('txnTime', None)

    assert nym_response == get_txn_response[DATA]
