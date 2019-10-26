import pytest

from plenum.common.constants import TARGET_NYM, DATA, ALIAS, SERVICES, TXN_TYPE, NODE
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
def future_primaries(node):
    fp = FuturePrimariesBatchHandler(FakeSomething(), node)
    return fp


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

