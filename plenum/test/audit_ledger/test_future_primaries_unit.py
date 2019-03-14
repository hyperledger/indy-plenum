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
    n.nodeIds = {'nym1': 'n1', 'nym2': 'n2', 'nym3': 'n3',
                 'nym4': 'n4', 'nym5': 'n5', 'nym6': 'n6'}
    n.primaries = {'n1', 'n2'}
    n.elector = FakeSomething()
    n.elector.process_selection = lambda a, b, c: ['n1', 'n2']
    return n


@pytest.fixture(scope='function')
def future_primaries(node):
    fp = FuturePrimariesBatchHandler(FakeSomething(), node)
    return fp


@pytest.fixture(scope='function')
def three_pc_batch():
    fp = ThreePcBatch(0, 0, 0, 3, 1, 'state', 'txn',
                      ['a', 'b', 'c'], ['a'])
    return fp


def test_add_node_empty_states(future_primaries, node, three_pc_batch):
    future_primaries.post_batch_applied(three_pc_batch)
    states = future_primaries.node_states
    assert len(states) == 1
    node_reg = list(node.nodeReg.keys())
    node_reg.append('n7')
    node.elector.process_selection = lambda a, b, c: ['n1', 'n2', 'n3']
    assert node_reg == states[0].node_reg


def test_add_and_demote_node(future_primaries, node, three_pc_batch):
    future_primaries.post_batch_applied(three_pc_batch)
    node_reg = list(node.nodeReg.keys())
    node_reg.append('n7')
    node.elector.process_selection = lambda a, b, c: ['n1', 'n2', 'n3']

    node.requests['a'].request.operation[DATA][SERVICES] = []
    future_primaries.post_batch_applied(three_pc_batch)

    states = future_primaries.node_states
    assert len(states) == 2
    assert list(node.nodeReg.keys()) == states[-1].node_reg


def test_apply_and_commit_1(future_primaries, node, three_pc_batch):
    future_primaries.post_batch_applied(three_pc_batch)
    node_reg = list(node.nodeReg.keys())
    node_reg.append('n7')
    node.elector.process_selection = lambda a, b, c: ['n1', 'n2', 'n3']

    future_primaries.set_node_state()
    assert len(future_primaries.node_states) == 1


def test_apply_and_commit_2(future_primaries, node, three_pc_batch):
    future_primaries.post_batch_applied(three_pc_batch)
    node_reg = list(node.nodeReg.keys())
    node_reg.append('n7')
    node.elector.process_selection = lambda a, b, c: ['n1', 'n2', 'n3']

    node.requests['a'].request.operation[DATA][SERVICES] = []
    future_primaries.post_batch_applied(three_pc_batch)

    future_primaries.set_node_state()
    assert len(future_primaries.node_states) == 1
    assert future_primaries.node_states[0].node_reg == list(node.nodeReg.keys())


def test_apply_and_revert(future_primaries, node, three_pc_batch):
    future_primaries.post_batch_applied(three_pc_batch)
    node_reg = list(node.nodeReg.keys())
    node_reg.append('n7')
    node.elector.process_selection = lambda a, b, c: ['n1', 'n2', 'n3']

    node.requests['a'].request.operation[DATA][SERVICES] = []
    future_primaries.post_batch_applied(three_pc_batch)

    future_primaries.post_batch_rejected(0)
    assert len(future_primaries.node_states) == 1
    node_reg = list(node.nodeReg.keys())
    node_reg.append('n7')
    assert future_primaries.node_states[0].node_reg == node_reg
