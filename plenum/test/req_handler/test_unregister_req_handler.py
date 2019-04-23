import functools
import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.ledger import Ledger
from plenum.server.node import Node
from plenum.test.plugin.demo_plugin.auction_req_handler import AuctionReqHandler
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture()
def fake_node():
    node = FakeSomething(txn_type_to_req_handler={},
                         txn_type_to_ledger_id={},
                         ledger_to_req_handler={})
    node.register_req_handler = functools.partial(Node.register_req_handler, node)
    node.unregister_req_handler = functools.partial(Node.unregister_req_handler, node)
    node.register_txn_type = functools.partial(Node.register_txn_type, node)
    node.unregister_txn_type = functools.partial(Node.unregister_txn_type, node)
    return node


@pytest.fixture()
def fake_ledger(tdir_for_func):
    return Ledger(CompactMerkleTree(), dataDir=tdir_for_func)


@pytest.fixture()
def fake_state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture()
def fake_req_handler(fake_ledger, fake_state):
    return AuctionReqHandler(fake_ledger,
                             fake_state)


def test_register_req_handler(fake_node,
                              fake_req_handler):
    ledger_id = 42
    fake_req_handler.write_types = {1, 2, 3}
    fake_node.register_req_handler(fake_req_handler, ledger_id)

    assert fake_node.ledger_to_req_handler
    assert fake_node.ledger_to_req_handler[42] == fake_req_handler

    assert fake_node.txn_type_to_req_handler
    assert fake_node.txn_type_to_req_handler[1] == fake_req_handler
    assert fake_node.txn_type_to_req_handler[2] == fake_req_handler
    assert fake_node.txn_type_to_req_handler[3] == fake_req_handler

    assert fake_node.txn_type_to_ledger_id
    assert fake_node.txn_type_to_ledger_id[1] == ledger_id
    assert fake_node.txn_type_to_ledger_id[2] == ledger_id
    assert fake_node.txn_type_to_ledger_id[3] == ledger_id


def test_unregister_req_handler(fake_node,
                                fake_req_handler):
    ledger_id = 42
    fake_req_handler.write_types = {1, 2, 3}
    fake_node.register_req_handler(fake_req_handler, ledger_id)

    fake_node.unregister_req_handler(fake_req_handler, ledger_id)
    assert 42 not in fake_node.ledger_to_req_handler
    assert len(fake_node.txn_type_to_req_handler) == 0
    assert len(fake_node.txn_type_to_ledger_id) == 0


def test_raise_exception_on_double_register(fake_node,
                                            fake_req_handler):
    ledger_id = 42
    fake_req_handler.write_types = {1, 2, 3}
    fake_node.register_req_handler(fake_req_handler, ledger_id)

    with pytest.raises(ValueError, match="already registered for"):
        fake_node.register_req_handler(fake_req_handler, ledger_id)


def test_raise_exception_on_double_unregister(fake_node,
                                              fake_req_handler):
    ledger_id = 42
    fake_req_handler.write_types = {1, 2, 3}
    fake_node.register_req_handler(fake_req_handler, ledger_id)

    fake_node.unregister_req_handler(fake_req_handler)
    with pytest.raises(ValueError, match="is not registered"):
        fake_node.unregister_req_handler(fake_req_handler, ledger_id)
