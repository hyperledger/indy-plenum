import pytest

from plenum.common.messages.node_messages import CatchupReq
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check

logger = getlogger()
leger_id = 1


def test_receive_incorrect_catchup_request_with_end_greater_catchuptill(looper,
                                                                        txnPoolNodeSet,
                                                                        sdk_pool_handle,
                                                                        sdk_wallet_client):
    end = 15
    catchup_till = 10
    req = CatchupReq(leger_id, 0, end, catchup_till)
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              4)
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    ledger_manager.processCatchupReq(req, "frm")
    _check_call_discard(ledger_manager, "not able to service since "
                                        "end = {} greater than "
                                        "catchupTill = {}"
                        .format(end, catchup_till))


def test_receive_incorrect_catchup_request_with_start_greater_end(looper,
                                                                  txnPoolNodeSet,
                                                                  sdk_pool_handle,
                                                                  sdk_wallet_client):
    start = 10
    end = 5
    req = CatchupReq(leger_id, start, end, 11)
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              4)
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    ledger_manager.processCatchupReq(req, "frm")
    _check_call_discard(ledger_manager, "not able to service since "
                                        "start = {} greater than "
                                        "end = {}"
                        .format(start, end))


def test_receive_incorrect_catchup_request_with_catchuptill_greater_ledger_size(
        looper,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client):
    catchup_till = 100
    req = CatchupReq(leger_id, 0, 10, catchup_till)
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              4)
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    ledger_manager.processCatchupReq(req, "frm")
    ledger_size = ledger_manager.getLedgerForMsg(req).size
    _check_call_discard(ledger_manager, "not able to service since "
                                        "catchupTill = {} greater than "
                                        "ledger size = {}"
                        .format(catchup_till, ledger_size))


def _check_call_discard(ledger_manager, discard_reason):
    last = ledger_manager.owner.spylog.getLastParams(
        ledger_manager.owner.discard,
        required=False)
    assert last
    assert discard_reason in last['reason']
