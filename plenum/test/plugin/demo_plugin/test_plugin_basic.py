from plenum.test.helper import waitForSufficientRepliesForRequests, \
    checkReqNackWithReason
from plenum.common.constants import CURRENT_PROTOCOL_VERSION, TXN_TYPE, DATA
from plenum.common.request import Request
from plenum.common.util import randomString
from plenum.test.plugin.demo_plugin import AUCTION_LEDGER_ID, dummy_field_length
from plenum.test.plugin.demo_plugin.constants import GET_BAL
from stp_core.loop.eventually import eventually


def test_plugin_setup(txnPoolNodeSet):
    """
    Test that plugin's ledger and state are setup
    """
    for node in txnPoolNodeSet:
        assert AUCTION_LEDGER_ID in node.ledger_ids
        assert AUCTION_LEDGER_ID in node.ledgerManager.ledgerRegistry
        assert node.ledger_ids == node.ledgerManager.ledger_sync_order
        assert AUCTION_LEDGER_ID in node.states


def test_plugin_client_req_fields(txnPoolNodeSet, looper, stewardWallet,
                                  steward1, client1Connected):
    """
    Test that plugin's addition of request fields and their validation is
    successful
    """
    op = {
        TXN_TYPE: GET_BAL,
        DATA: {'id': '123'}
    }

    # Valid field value results in successful processing
    req = Request(operation=op, reqId=Request.gen_req_id(),
                  protocolVersion=CURRENT_PROTOCOL_VERSION,
                  identifier=stewardWallet.defaultId,
                  fix_length_dummy=randomString(dummy_field_length))
    req = stewardWallet.signRequest(req)
    steward1.submitReqs(req)
    waitForSufficientRepliesForRequests(looper, steward1, requests=[req, ])

    # Invalid field value results in proper failure
    req = Request(operation=op, reqId=Request.gen_req_id(),
                  protocolVersion=CURRENT_PROTOCOL_VERSION,
                  identifier=stewardWallet.defaultId,
                  fix_length_dummy=randomString(dummy_field_length+1))
    steward1.submitReqs(req)
    for node in txnPoolNodeSet:
        looper.run(eventually(checkReqNackWithReason, steward1,
                              'should have length',
                              node.clientstack.name, retryWait=1))
