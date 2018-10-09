from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.messages.node_messages import CatchupRep
from plenum.common.txn_util import append_txn_metadata, reqToTxn
from plenum.common.types import f
from plenum.common.util import SortedDict
from plenum.test.helper import sdk_signed_random_requests

ledger_id = DOMAIN_LEDGER_ID


def _add_txns_to_ledger(node, looper, sdk_wallet_client, num_txns_in_reply, reply_count):
    '''
    Add txn_count transactions to node's ledger and return
    ConsistencyProof for all new transactions and list of CatchupReplies
    :return: ConsistencyProof, list of CatchupReplies
    '''
    txn_count = num_txns_in_reply * reply_count
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
    for i in range(ledger.seqNo - txn_count + 1, ledger.seqNo + 1, num_txns_in_reply):
        start = i
        end = i + num_txns_in_reply - 1
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


def check_reply_not_applied(old_ledger_size, ledger, ledger_info, reply):
    assert ledger.size == old_ledger_size
    assert ledger.seqNo == old_ledger_size
    received_replies = {str(seq_no) for seq_no, _ in ledger_info.receivedCatchUpReplies}
    assert set(getattr(reply, f.TXNS.nm).keys()).issubset(received_replies)


def check_replies_applied(old_ledger_size, ledger, ledger_info, replies):
    new_txn_count = sum([len(getattr(reply, f.TXNS.nm).keys())
                         for reply in replies])
    assert ledger.size == old_ledger_size + new_txn_count
    assert ledger.seqNo == old_ledger_size + new_txn_count
    received_replies = {str(seq_no) for seq_no, _ in ledger_info.receivedCatchUpReplies}
    assert all(not set(getattr(reply, f.TXNS.nm).keys()).issubset(received_replies)
               for reply in replies)


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
    num_txns_in_reply = 3
    reply_count = 6

    cons_proof, catchup_reps = _add_txns_to_ledger(txnPoolNodeSet[1],
                                                   looper,
                                                   sdk_wallet_client,
                                                   num_txns_in_reply,
                                                   reply_count)
    ledger_info.catchUpTill = cons_proof
    ledger_info.state = LedgerState.syncing

    # send replies in next order: 2, 3, 1
    # and check that after sending reply 1, replies 2 and 3 will be applied.
    reply2 = catchup_reps[1]
    ledger_manager.processCatchupRep(reply2, sdk_wallet_client[1])
    check_reply_not_applied(old_ledger_size, ledger, ledger_info, reply2)

    reply3 = catchup_reps[2]
    ledger_manager.processCatchupRep(reply3, sdk_wallet_client[1])
    check_reply_not_applied(old_ledger_size, ledger, ledger_info, reply2)

    reply1 = catchup_reps[0]
    ledger_manager.processCatchupRep(reply1, sdk_wallet_client[1])
    check_replies_applied(old_ledger_size,
                          ledger,
                          ledger_info,
                          [reply1, reply2, reply3])
    old_ledger_size = ledger.size

    # send replies in next order: 6, 4, 5
    # and check that after sending reply 4, it will be applied.
    # Check that after sending reply 5, replies 5 and 6 will be applied.
    reply6 = catchup_reps[5]
    ledger_manager.processCatchupRep(reply6, sdk_wallet_client[1])
    check_reply_not_applied(old_ledger_size, ledger, ledger_info, reply6)

    reply4 = catchup_reps[3]
    ledger_manager.processCatchupRep(reply4, sdk_wallet_client[1])
    check_replies_applied(old_ledger_size, ledger, ledger_info, [reply4])
    old_ledger_size = ledger.size

    reply5 = catchup_reps[4]
    ledger_manager.processCatchupRep(reply5, sdk_wallet_client[1])
    check_replies_applied(old_ledger_size, ledger, ledger_info, [reply5, reply6])

    assert not ledger_info.receivedCatchUpReplies


def test_process_invalid_catchup_reply(txnPoolNodeSet, looper, sdk_wallet_client):
    '''
    Test correct work of method processCatchupRep and that sending replies
    in reverse order will call a few iterations of cycle in _processCatchupRep
    '''
    # count of transactions and replies (one txn in one reply)
    # and iterations in _processCatchupRep
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    ledger_info = ledger_manager.getLedgerInfoByType(ledger_id)
    ledger = ledger_manager.ledgerRegistry[ledger_id].ledger
    old_ledger_size = ledger.size
    num_txns_in_reply = 3
    reply_count = 1

    cons_proof, catchup_reps = _add_txns_to_ledger(txnPoolNodeSet[1],
                                                   looper,
                                                   sdk_wallet_client,
                                                   num_txns_in_reply,
                                                   reply_count)
    ledger_info.catchUpTill = cons_proof
    ledger_info.state = LedgerState.syncing

    # make invalid catchup reply by dint of adding new transaction in it
    reply = catchup_reps[0]
    txns = getattr(reply, f.TXNS.nm)
    req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)[0]
    txns[str(old_ledger_size + 4)] = append_txn_metadata(reqToTxn(req), txn_time=12345678)
    ledger_manager.processCatchupRep(CatchupRep(ledger_id,
                                                txns,
                                                getattr(reply, f.CONS_PROOF.nm)),
                                     sdk_wallet_client[1])
    # check that transactions was not ordered
    assert ledger.size == old_ledger_size
    assert ledger.seqNo == old_ledger_size
    received_replies = {str(seq_no) for seq_no, _ in ledger_info.receivedCatchUpReplies}
    assert not set(getattr(reply, f.TXNS.nm).keys()).issubset(received_replies)
