from collections import OrderedDict

from plenum.common.constants import DOMAIN_LEDGER_ID, LedgerState
from plenum.common.ledger import Ledger
from plenum.common.messages.node_messages import CatchupRep
from plenum.common.txn_util import append_txn_metadata, reqToTxn
from plenum.common.types import f
from plenum.common.util import SortedDict
from plenum.server.catchup.utils import CatchupTill
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
    catchup_rep_service = ledger_manager._node_leecher._leechers[ledger_id]._catchup_rep_service
    reqs = sdk_signed_random_requests(looper, sdk_wallet_client, txn_count)
    # add transactions to ledger
    for req in reqs:
        txn = append_txn_metadata(reqToTxn(req), txn_time=12345678)
        catchup_rep_service._add_txn(txn)
    # generate CatchupReps
    replies = []
    for i in range(ledger.seqNo - txn_count + 1, ledger.seqNo + 1, num_txns_in_reply):
        start = i
        end = i + num_txns_in_reply - 1
        cons_proof = ledger_manager._node_seeder._make_consistency_proof(ledger, end, ledger.size)
        txns = {}
        for seq_no, txn in ledger.getAllTxn(start, end):
            txns[str(seq_no)] = ledger_manager.owner.update_txn_with_extra_data(txn)
        replies.append(CatchupRep(ledger_id,
                                  SortedDict(txns),
                                  cons_proof))

    three_pc_key = node.three_phase_key_for_txn_seq_no(ledger_id, ledger.seqNo)
    view_no, pp_seq_no = three_pc_key if three_pc_key else (0, 0)
    return CatchupTill(start_size=ledger.seqNo - txn_count,
                       final_size=ledger.seqNo,
                       final_hash=Ledger.hashToStr(ledger.tree.merkle_tree_hash(0, ledger.seqNo)),
                       view_no=view_no,
                       pp_seq_no=pp_seq_no), replies


def check_reply_not_applied(old_ledger_size, ledger, catchup_rep_service, frm, reply):
    assert ledger.size == old_ledger_size
    assert ledger.seqNo == old_ledger_size
    received_replies = {str(seq_no) for seq_no, _ in catchup_rep_service._received_catchup_txns}
    assert set(reply.txns.keys()).issubset(received_replies)
    assert reply in catchup_rep_service._received_catchup_replies_from[frm]


def check_replies_applied(old_ledger_size, ledger, catchup_rep_service, frm, replies):
    new_txn_count = sum([len(getattr(reply, f.TXNS.nm).keys())
                         for reply in replies])
    assert ledger.size == old_ledger_size + new_txn_count
    assert ledger.seqNo == old_ledger_size + new_txn_count
    received_replies = {str(seq_no) for seq_no, _ in catchup_rep_service._received_catchup_txns}
    assert all(not set(getattr(reply, f.TXNS.nm).keys()).issubset(received_replies)
               for reply in replies)
    assert frm not in catchup_rep_service._received_catchup_replies_from or \
           all(reply not in catchup_rep_service._received_catchup_replies_from[frm]
               for reply in replies)
    return ledger.size


def test_process_catchup_replies(txnPoolNodeSet, looper, sdk_wallet_client):
    '''
    Test correct work of method processCatchupRep and that sending replies
    in reverse order will call a few iterations of cycle in _processCatchupRep
    '''
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    catchup_rep_service = ledger_manager._node_leecher._leechers[ledger_id]._catchup_rep_service
    ledger = ledger_manager.ledgerRegistry[ledger_id].ledger
    ledger_size = ledger.size
    num_txns_in_reply = 3
    reply_count = 6

    catchup_till, catchup_reps = _add_txns_to_ledger(txnPoolNodeSet[1],
                                                     looper,
                                                     sdk_wallet_client,
                                                     num_txns_in_reply,
                                                     reply_count)
    catchup_rep_service._catchup_till = catchup_till
    catchup_rep_service._is_working = True

    # send replies in next order: 2, 3, 1
    # and check that after sending reply 1, replies 2 and 3 will be applied.
    reply2 = catchup_reps[1]
    ledger_manager.processCatchupRep(reply2, sdk_wallet_client[1])
    check_reply_not_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], reply2)

    reply3 = catchup_reps[2]
    ledger_manager.processCatchupRep(reply3, sdk_wallet_client[1])
    check_reply_not_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], reply2)
    check_reply_not_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], reply3)

    reply1 = catchup_reps[0]
    ledger_manager.processCatchupRep(reply1, sdk_wallet_client[1])
    ledger_size = check_replies_applied(ledger_size,
                          ledger,
                          catchup_rep_service,
                          sdk_wallet_client[1],
                          [reply1, reply2, reply3])

    # send replies in next order: 6, 4, 5
    # and check that after sending reply 4, it will be applied.
    # Check that after sending reply 5, replies 5 and 6 will be applied.
    reply6 = catchup_reps[5]
    ledger_manager.processCatchupRep(reply6, sdk_wallet_client[1])
    check_reply_not_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], reply6)

    reply4 = catchup_reps[3]
    ledger_manager.processCatchupRep(reply4, sdk_wallet_client[1])
    ledger_size = check_replies_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], [reply4])
    check_reply_not_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], reply6)

    reply5 = catchup_reps[4]
    ledger_manager.processCatchupRep(reply5, sdk_wallet_client[1])
    ledger_size = check_replies_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], [reply5,
                                                                                       reply6])
    assert not catchup_rep_service._received_catchup_replies_from
    assert not catchup_rep_service._received_catchup_txns


def test_process_invalid_catchup_reply(txnPoolNodeSet, looper, sdk_wallet_client):
    '''
    Test correct work of method processCatchupRep and that sending replies
    in reverse order will call a few iterations of cycle in _processCatchupRep
    '''
    ledger_manager = txnPoolNodeSet[0].ledgerManager
    catchup_rep_service = ledger_manager._node_leecher._leechers[ledger_id]._catchup_rep_service
    ledger = ledger_manager.ledgerRegistry[ledger_id].ledger
    ledger_size = ledger.size
    num_txns_in_reply = 3
    reply_count = 2

    catchup_till, catchup_reps = _add_txns_to_ledger(txnPoolNodeSet[1],
                                                     looper,
                                                     sdk_wallet_client,
                                                     num_txns_in_reply,
                                                     reply_count)
    catchup_rep_service._catchup_till = catchup_till
    catchup_rep_service._is_working = True

    # make invalid catchup reply by dint of adding new transaction in it
    reply2 = catchup_reps[1]
    txns = OrderedDict(getattr(reply2, f.TXNS.nm))
    req = sdk_signed_random_requests(looper, sdk_wallet_client, 1)[0]
    txns[str(ledger_size + 4)] = append_txn_metadata(reqToTxn(req), txn_time=12345678)
    invalid_reply2 = CatchupRep(ledger_id,
                                txns,
                                getattr(reply2, f.CONS_PROOF.nm))
    # process 2nd interval with invalid catchup reply
    ledger_manager.processCatchupRep(invalid_reply2,
                                     sdk_wallet_client[1])
    # check that invalid transaction was not added to ledger, but add to ledger_info.receivedCatchUpReplies
    check_reply_not_applied(ledger_size, ledger, catchup_rep_service, sdk_wallet_client[1], invalid_reply2)

    # process valid reply from 1st interval
    reply1 = catchup_reps[0]
    ledger_manager.processCatchupRep(reply1, sdk_wallet_client[1])
    # check that only valid reply added to ledger
    ledger_size = check_replies_applied(ledger_size,
                          ledger,
                          catchup_rep_service,
                          sdk_wallet_client[1],
                          [reply1])
    # check that invalid reply was removed from ledger_info.receivedCatchUpReplies
    received_replies = {str(seq_no) for seq_no, _ in catchup_rep_service._received_catchup_txns}
    assert not set(reply2.txns.keys()).issubset(received_replies)
    assert not catchup_rep_service._received_catchup_replies_from[sdk_wallet_client[1]]

    # check that valid reply for 2nd interval was added to ledger
    reply2 = catchup_reps[1]
    ledger_manager.processCatchupRep(reply2, sdk_wallet_client[1])
    ledger_size = check_replies_applied(ledger_size,
                          ledger,
                          catchup_rep_service,
                          sdk_wallet_client[1],
                          [reply2])
    assert not catchup_rep_service._received_catchup_replies_from
    assert not catchup_rep_service._received_catchup_txns
