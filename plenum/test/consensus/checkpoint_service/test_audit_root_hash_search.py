import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.constants import AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, TXN_METADATA, TXN_PAYLOAD, TXN_PAYLOAD_DATA
from plenum.common.ledger import Ledger
from plenum.server.consensus.checkpoint_service import CheckpointService


@pytest.fixture(params=[
    [],
    [(0, 1)],
    [(2, 3)],
    [(0, 1), (0, 2), (0, 3)],
    [(0, 1), (0, 2), (0, 3), (1, 4)],
    [(0, 1), (0, 2), (2, 3), (4, 4)]
])
def ordered_batches(request):
    return request.param


@pytest.fixture
def audit_ledger(tconf, tmpdir_factory, ordered_batches):
    tdir = tmpdir_factory.mktemp('').strpath
    ledger = Ledger(CompactMerkleTree(), dataDir=tdir)
    for view_no, pp_seq_no in ordered_batches:
        txn = {
            TXN_PAYLOAD: {
                TXN_PAYLOAD_DATA: {
                    AUDIT_TXN_VIEW_NO: view_no,
                    AUDIT_TXN_PP_SEQ_NO: pp_seq_no
                }
            },
            TXN_METADATA: {}
        }
        ledger.append_txns_metadata([txn], 0)
        ledger.append(txn)
    return ledger


def test_search_existing_key_in_audit_ledger_returns_its_index(audit_ledger, ordered_batches):
    for expected_seq_no, (view_no, pp_seq_no) in enumerate(ordered_batches, start=1):
        _, actual_seq_no = CheckpointService._audit_txn_by_pp_seq_no(audit_ledger, pp_seq_no)
        assert expected_seq_no == actual_seq_no


def test_search_key_after_last_in_audit_ledger_returns_zero(audit_ledger, ordered_batches):
    last_view_no, last_pp_seq_no = ordered_batches[-1] if ordered_batches else (0, 0)
    for view_no_inc, pp_seq_no_inc in [(0, 1), (0, 2), (1, 1)]:
        pp_seq_no = last_pp_seq_no + pp_seq_no_inc
        _, seq_no = CheckpointService._audit_txn_by_pp_seq_no(audit_ledger, pp_seq_no)
        assert seq_no == 0


def test_search_key_before_first_in_audit_ledger_returns_zero(audit_ledger, ordered_batches):
    last_view_no, last_pp_seq_no = ordered_batches[0] if ordered_batches else (0, 0)
    for view_no_dec, pp_seq_no_dec in [(0, 1), (0, 2), (1, 1)]:
        pp_seq_no = last_pp_seq_no - pp_seq_no_dec
        _, seq_no = CheckpointService._audit_txn_by_pp_seq_no(audit_ledger, pp_seq_no)
        assert seq_no == 0
