import pytest

from plenum.common.constants import NYM, DOMAIN_LEDGER_ID
from plenum.common.txn_util import init_empty_txn

ledger_id = DOMAIN_LEDGER_ID


def add_txns(node, num):
    ledger = node.getLedger(ledger_id)
    for i in range(num):
        ledger.append(init_empty_txn(NYM))


@pytest.yield_fixture("module")
def node_with_tnxs(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    add_txns(node, 20)
    return node


@pytest.yield_fixture("function")
def node(node_with_tnxs):
    node_with_tnxs.txn_seq_range_to_3phase_key = {}
    return node_with_tnxs


def test_update_txn_seq_range_to_3phase_after_catchup(node):
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (3, 1))
    ledger_size = node.getLedger(ledger_id).size

    assert (3, 1) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size)
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size - 1) is None
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size + 1) is None


def test_update_txn_seq_range_to_3phase_after_catchup_multiple_times_same_size(node):
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (3, 1))
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (5, 1))
    ledger_size = node.getLedger(ledger_id).size

    assert (3, 1) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size)
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size - 1) is None
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size + 1) is None


def test_update_txn_seq_range_to_3phase_after_catchup_multiple_times_diff_size(node):
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (3, 1))
    add_txns(node, 1)
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (5, 1))
    add_txns(node, 1)
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (5, 2))

    ledger_size = node.getLedger(ledger_id).size
    assert (5, 2) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size)
    assert (5, 1) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size - 1)
    assert (3, 1) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size - 2)
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size + 1) is None
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size - 3) is None


def test_test_update_txn_seq_range_to_3phase_after_catchup_does_not_update_existing(node):
    node._update_txn_seq_range_to_3phase(first_txn_seq_no=0,
                                         last_txn_seq_no=1000,
                                         ledger_id=ledger_id,
                                         view_no=5,
                                         pp_seq_no=155)

    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (5, 1))

    ledger_size = node.getLedger(ledger_id).size
    assert (5, 155) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size)
    assert (5, 155) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size - 1)
    assert (5, 155) == node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size + 1)


def test_update_txn_seq_range_to_3phase_after_catchup_does_not_update_initial(node):
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, None)
    ledger_size = node.getLedger(ledger_id).size
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size) is None


def test_update_txn_seq_range_to_3phase_after_catchup_does_not_update_zero_pp_seqno(node):
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (3, 0))
    ledger_size = node.getLedger(ledger_id).size
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size) is None


def test_update_txn_seq_range_to_3phase_after_catchup_does_not_update_0_0(node):
    # (0,0) can be passed from ledger_manager._buildConsistencyProof if 3PC is None
    # we should not process (0, 0)
    node._update_txn_seq_range_to_3phase_after_catchup(ledger_id, (0, 0))
    ledger_size = node.getLedger(ledger_id).size
    assert node.three_phase_key_for_txn_seq_no(ledger_id, ledger_size) is None
