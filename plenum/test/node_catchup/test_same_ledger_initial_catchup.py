from types import MethodType

import pytest

# noinspection PyUnresolvedReferences
from ledger.test.conftest import tempdir, txn_serializer, hash_serializer  # noqa
from plenum.common.channel import Router
from plenum.common.constants import LedgerState, CURRENT_PROTOCOL_VERSION
from plenum.common.messages.node_messages import LedgerStatus
from plenum.server.catchup.utils import NodeCatchupComplete, LedgerCatchupComplete

nodeCount = 7

ledger_id = 1


@pytest.fixture(scope="module")
def patched_node(txnPoolNodeSet):
    node = txnPoolNodeSet[0]

    node_leecher = node.ledgerManager._node_leecher
    router = Router(node_leecher._leecher_outbox_rx)

    def patched_ledger_catchup_complete(msg):
        node_leecher._output.put_nowait(NodeCatchupComplete())

    router.add(LedgerCatchupComplete, patched_ledger_catchup_complete)

    return node


@pytest.fixture(scope="function")
def node_and_leecher(patched_node):
    '''
    Emulate restart of the node (clean state)
    '''
    patched_node.txn_seq_range_to_3phase_key = {}
    patched_node.master_replica.last_ordered_3pc = (0, 0)

    view_changer = patched_node.view_changer
    view_changer.propagate_primary = True
    view_changer.view_no = 0
    view_changer.view_change_in_progress = True
    view_changer.set_defaults()

    ledger_manager = patched_node.ledgerManager
    ledger_manager.last_caught_up_3PC = (0, 0)

    leecher = ledger_manager._node_leecher._leechers[ledger_id]
    leecher.start(request_ledger_statuses=False)

    ledger_status = patched_node.build_ledger_status(ledger_id)
    assert ledger_status.viewNo is None
    assert ledger_status.ppSeqNo is None

    return patched_node, leecher, ledger_status, leecher._cons_proof_service


def test_same_ledger_status_quorum(txnPoolNodeSet,
                                   node_and_leecher):
    '''
    Check that we require at least n-f-1 (=4) same LedgerStatus msgs
    to finish CatchUp
    '''
    node, leecher, ledger_status, cons_proof_service = node_and_leecher

    status_from = set()
    for i in range(3):
        node_name = txnPoolNodeSet[i + 1].name
        cons_proof_service.process_ledger_status(ledger_status, node_name)
        status_from = status_from.union({node_name})
        assert cons_proof_service._same_ledger_status == status_from
        assert leecher.state == LedgerState.not_synced

    node_name = txnPoolNodeSet[4].name
    cons_proof_service.process_ledger_status(ledger_status, node_name)

    assert cons_proof_service._same_ledger_status == set()
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_same_3PC(txnPoolNodeSet,
                                                  node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same not None 3PC keys
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher
    ledger_status_2_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      2, 20,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_2_40, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_2_40, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_2_40, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_2_40, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (2, 20)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_same_None_3PC(txnPoolNodeSet,
                                                       node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same None 3PC keys (like at the initial start of the pool)
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_one_not_none_3PC_last(txnPoolNodeSet,
                                                               node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same None 3PC keys except the last one.
    The last msg contains not None 3PC, but it's not enough for setting last_ordered_3PC
    since the quorum is f+1 (=3)
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    ledger_status_3_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      3, 40,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_one_not_none_3PC_first(txnPoolNodeSet,
                                                                node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same None 3PC keys except the first one.
    The first msg contains not None 3PC, but it's not enough for setting last_ordered_3PC
    since the quorum is f+1 (=3)
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    ledger_status_3_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      3, 40,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_not_none_3PC_quorum_with_none(txnPoolNodeSet,
                                                                       node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same not None 3PC keys except the last one.
    The last msg contains None 3PC, but not None from the previous msgs is used
    since we have a quorum of f+1 (=3)
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    ledger_status_3_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      3, 40,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_none_3PC, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (3, 40)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_not_none_3PC_quorum1(txnPoolNodeSet,
                                                              node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same not None 3PC keys except the last one.
    The last msg contains a different not None 3PC, but 3PC from the previous msgs is used
    since we have a quorum of f+1 (=3)
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    ledger_status_1_10 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      1, 10,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    ledger_status_3_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      3, 40,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_1_10, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_1_10, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_1_10, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (1, 10)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_not_none_3PC_quorum2(txnPoolNodeSet,
                                                              node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs
    if all LedgerStatus msgs have the same not None 3PC keys except the last one.
    The last msg contains a different not None 3PC, but 3PC from the previous msgs is used
    since we have a quorum of f+1 (=3)
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    ledger_status_1_10 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      1, 10,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    ledger_status_3_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      3, 40,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_1_10, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (3, 40)
    assert leecher.state == LedgerState.synced


def test_same_ledger_status_last_ordered_not_none_3PC_no_quorum_equal(txnPoolNodeSet,
                                                                      node_and_leecher):
    '''
    Check that last_ordered_3PC is set according to 3PC from LedgerStatus msgs.
    Check that if we have no quorum (2 different keys, but 3 is required ror quorum), then
    they are not used.
    '''
    node, leecher, ledger_status_none_3PC, cons_proof_service = node_and_leecher

    ledger_status_1_10 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      1, 10,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    ledger_status_3_40 = LedgerStatus(ledger_status_none_3PC.ledgerId,
                                      ledger_status_none_3PC.txnSeqNo,
                                      3, 40,
                                      ledger_status_none_3PC.merkleRoot,
                                      CURRENT_PROTOCOL_VERSION)

    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[1].name)
    cons_proof_service.process_ledger_status(ledger_status_3_40, txnPoolNodeSet[2].name)
    cons_proof_service.process_ledger_status(ledger_status_1_10, txnPoolNodeSet[3].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.not_synced

    cons_proof_service.process_ledger_status(ledger_status_1_10, txnPoolNodeSet[4].name)
    assert node.master_last_ordered_3PC == (0, 0)
    assert leecher.state == LedgerState.synced
