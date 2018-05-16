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

    def check_msg(msg: CatchupRep, to, message_splitter):
        assert msg.consProof == []
        assert msg.ledgerId == leger_id

    monkeypatch.setattr(ledger_manager, 'sendTo', check_msg)
    req = CatchupReq(leger_id, 0, 15, 10)
    ledger_manager.processCatchupReq(req, "frm")
