import logging

import pytest

from plenum.common.messages.node_messages import CatchupReq, CatchupRep
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check

logger = getlogger()
leger_id = 1


def test_receive_incorrect_catchup_request_with_end_greater_catchuptill(looper,
                                                                        txnPoolNodeSet,
                                                                        sdk_pool_handle,
                                                                        sdk_wallet_client,
                                                                        monkeypatch):
    req = CatchupReq(leger_id, 0, 15, 10)
    _process_catchup_req(req,
                         looper,
                         txnPoolNodeSet,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         monkeypatch)


def test_receive_incorrect_catchup_request_with_start_greater_end(looper,
                                                                  txnPoolNodeSet,
                                                                  sdk_pool_handle,
                                                                  sdk_wallet_client,
                                                                  monkeypatch):
    req = CatchupReq(leger_id, 10, 5, 11)
    _process_catchup_req(req,
                         looper,
                         txnPoolNodeSet,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         monkeypatch)


def test_receive_incorrect_catchup_request_with_catchuptill_greater_ledger_size(
        looper,
        txnPoolNodeSet,
        sdk_pool_handle,
        sdk_wallet_client,
        monkeypatch):
    req = CatchupReq(leger_id, 0, 10, 100)
    _process_catchup_req(req,
                         looper,
                         txnPoolNodeSet,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         monkeypatch)


def _process_catchup_req(req: CatchupReq,
                         looper,
                         txnPoolNodeSet,
                         sdk_pool_handle,
                         sdk_wallet_client,
                         monkeypatch):
    sdk_send_random_and_check(looper,
                              txnPoolNodeSet,
                              sdk_pool_handle,
                              sdk_wallet_client,
                              4)
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    monkeypatch.setattr(ledger_manager.owner, 'discard', _check_discard)
    ledger_manager.processCatchupReq(req, "frm")


def _check_discard(msg, reason, logMethod=logging.error, cliOutput=False):
    assert reason.find("not able to service since")
    assert reason.find("ledger size is")
