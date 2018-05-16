import logging

import pytest

from plenum.common.messages.node_messages import CatchupReq, CatchupRep
from stp_core.common.log import getlogger
from plenum.test.helper import sdk_send_random_and_check

logger = getlogger()


def test_receive_incorrect_catchup_request(looper,
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
    leger_id = 1

    def check_discard(msg, reason, logMethod=logging.error, cliOutput=False):
        assert reason.find("not able to service since")
        assert reason.find("ledger size is")

    monkeypatch.setattr(ledger_manager.owner, 'discard', check_discard)
    req = CatchupReq(leger_id, 0, 15, 10)
    ledger_manager.processCatchupReq(req, "frm")
