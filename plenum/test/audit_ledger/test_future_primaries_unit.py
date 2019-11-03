import pytest

from common.exceptions import LogicError
from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.constants import TARGET_NYM, DATA, ALIAS, SERVICES, TXN_TYPE, NODE, TXN_PAYLOAD, TXN_PAYLOAD_DATA, \
    AUDIT_TXN_VIEW_NO, AUDIT_TXN_PP_SEQ_NO, TXN_METADATA, AUDIT_TXN_PRIMARIES
from plenum.common.ledger import Ledger
from plenum.common.request import Request
from plenum.server.batch_handlers.three_pc_batch import ThreePcBatch
from plenum.server.future_primaries_batch_handler import FuturePrimariesBatchHandler
from plenum.server.propagator import ReqState
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='function')
def node():
    n = FakeSomething()
    n.new_future_primaries_needed = False
    n.requests = {'a': ReqState(Request(operation={TARGET_NYM: 'nym7',
                                                   TXN_TYPE: NODE,
                                                   DATA: {
                                                       SERVICES: ['VALIDATOR'],
                                                       ALIAS: 'n7'}
                                                   }
                                        ))}
    n.nodeReg = {'n1': 1, 'n2': 1, 'n3': 1,
                 'n4': 1, 'n5': 1, 'n6': 1}
    n.primaries = ['n1', 'n2']
    n.nodeIds = n.nodeReg
    n.primaries_selector = FakeSomething()
    n.primaries_selector.select_primaries = lambda view_no, instance_count, validators: ['n1', 'n2']
    n.viewNo = 0
    return n


@pytest.fixture(scope='function')
def future_primaries(node, audit_ledger):
    fp = FuturePrimariesBatchHandler(FakeSomething(get_ledger=lambda *args: audit_ledger), node)
    fp.primaries[node.viewNo] = list(node.primaries)
    return fp


@pytest.fixture(params=[[(0, 1, ['A', 'B']),
                         (0, 2, 1),
                         (0, 3, 2),
                         (1, 4, ['B', 'G']),
                         (1, 5, 1),
                         (2, 6, ['G', 'D']),
                         (2, 7, 1),
                         (3, 8, ['D', 'A']),
                         (3, 9, 1)],
                        [(0, 1, ['A', 'B']),
                         (1, 2, ['B', 'G']),
                         (2, 3, ['G', 'D']),
                         (3, 4, ['D', 'A'])]])
def ordered_batches(request):
    return request.param


@pytest.fixture(scope='function', params=['all_uncommitted', 'all_committed', 'mixed'])
def audit_ledger(tconf, tmpdir_factory, ordered_batches, request):
    tdir = tmpdir_factory.mktemp('').strpath
    ledger = Ledger(CompactMerkleTree(), dataDir=tdir)
    for view_no, pp_seq_no, primaries in ordered_batches:
        txn = {
            TXN_PAYLOAD: {
                TXN_PAYLOAD_DATA: {
                    AUDIT_TXN_VIEW_NO: view_no,
                    AUDIT_TXN_PP_SEQ_NO: pp_seq_no,
                    AUDIT_TXN_PRIMARIES: primaries,
                }
            },
            TXN_METADATA: {}
        }
        if request.param == "mixed":
            if view_no > 1:
                ledger.uncommittedTxns.append(txn)
            else:
                ledger.append(txn)
        elif request.param == "all_committed":
            ledger.append(txn)
        elif request.param == "all_uncommitted":
            ledger.uncommittedTxns.append(txn)
    return ledger


@pytest.fixture(scope='function')
def three_pc_batch():
    fp = ThreePcBatch(0, 0, 0, 3, 1, 'state', 'txn',
                      ['a', 'b', 'c'], ['a'], pp_digest='')
    return fp


def test_post_batch_applied(future_primaries, node, three_pc_batch):
    future_primaries.post_batch_applied(three_pc_batch)
    assert three_pc_batch.primaries == node.primaries


def test_post_batch_applied_during_reordering(future_primaries, node, three_pc_batch):
    primaries = list(node.primaries)
    node.primaries.append("n10")
    future_primaries.post_batch_applied(three_pc_batch)
    assert three_pc_batch.primaries == primaries


def test_get_primaries_by_view_no(future_primaries):
    assert future_primaries.get_primaries_from_audit(0) == ['A', 'B']
    assert future_primaries.get_primaries_from_audit(1) == ['B', 'G']
    assert future_primaries.get_primaries_from_audit(2) == ['G', 'D']
    assert future_primaries.get_primaries_from_audit(3) == ['D', 'A']
