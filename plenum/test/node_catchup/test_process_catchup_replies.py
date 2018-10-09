from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.messages.node_messages import CatchupRep
from plenum.common.txn_util import append_txn_metadata, reqToTxn
from plenum.common.types import f
from plenum.common.util import SortedDict
from plenum.test.helper import sdk_signed_random_requests

ledger_id = DOMAIN_LEDGER_ID


def _add_txns_to_ledger(node, txn_count, looper, sdk_wallet_client):
    '''
    Add txn_count transactions to node's ledger and return
    ConsistencyProof for all new transactions and list of CatchupReplies
    :return: ConsistencyProof, list of CatchupReplies
    '''
    ledger_manager = node.ledgerManager
    ledger = ledger_manager.ledgerRegistry[ledger_id].ledger
    ledger_info = ledger_manager.getLedgerInfoByType(ledger_id)
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, txn_count)
    # add transactions to ledger
    for req in reqs:
        txn = append_txn_metadata(reqToTxn(req), txn_time=12345678)
        ledger_manager._add_txn(
            ledger_id, ledger, ledger_info, txn)
    # generate CatchupReps
    replies = []
    for i in range(len(reqs)):
        start = ledger.seqNo - i
        end = ledger.seqNo - i
        cons_proof = ledger_manager._make_consistency_proof(ledger, end, ledger.size)
        txns = {}
        for seq_no, txn in ledger.getAllTxn(start, end):
            txns[str(seq_no)] = ledger_manager.owner.update_txn_with_extra_data(txn)
        replies.append(CatchupRep(ledger_id,
                                  SortedDict(txns),
                                  cons_proof))
    return ledger_manager._buildConsistencyProof(ledger_id,
                                                 ledger.seqNo - txn_count,
                                                 ledger.seqNo), replies


def test_process_catchup_replies(txnPoolNodeSet, looper, sdk_wallet_client):
    '''
    Test correct work of method processCatchupRep and that sending replies
    in reverse order will call a few iterations of cycle in _processCatchupRep
    '''
    # count of transactions and replies (one txn in one reply)
    # and iterations in _processCatchupRep
    txns_count = 3
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    ledger_info = ledger_manager.getLedgerInfoByType(ledger_id)
    ledger = ledger_manager.ledgerRegistry[ledger_id].ledger
    old_ledger_size = ledger.size

    cons_proof, catchup_reps = _add_txns_to_ledger(txnPoolNodeSet[1], txns_count, looper, sdk_wallet_client)
    ledger_info.catchUpTill = cons_proof
    for reply in catchup_reps:
        ledger_info.state = LedgerState.syncing
        ledger_manager.processCatchupRep(reply, sdk_wallet_client[1])
        received_replies = {str(seq_no) for seq_no, _ in ledger_info.receivedCatchUpReplies}
        assert catchup_reps[txns_count - 1] == reply or \
               set(getattr(reply, f.TXNS.nm).keys()).issubset(received_replies)
    assert ledger.size == ledger.seqNo == old_ledger_size + txns_count
    assert not ledger_info.receivedCatchUpReplies
