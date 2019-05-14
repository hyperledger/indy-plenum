import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.ledger import Ledger
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.delayers import req_delay
from plenum.test.testing_utils import FakeSomething
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture
def config_ledger(tmpdir_factory):
    tdir = tmpdir_factory.mktemp('').strpath
    return Ledger(CompactMerkleTree(),
                  dataDir=tdir)


@pytest.fixture
def config_state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture
def config_req_handler(config_state,
                       config_ledger):

    return ConfigReqHandler(config_ledger,
                            config_state,
                            domain_state=FakeSomething(),
                            bls_store=FakeSomething())


@pytest.fixture(scope="module")
def nodeSetWithOneNodeResponding(txnPoolNodeSet):
    # the order of nodes the client sends requests to is [Alpha, Beta, Gamma, Delta]
    # delay all requests to Beta, Gamma and Delta
    # we expect that it's sufficient for the client to get Reply from Alpha only
    # as for write requests, we can send it to 1 node only, and it will be propagated to others
    for node in txnPoolNodeSet[1:]:
        node.clientIbStasher.delay(req_delay())
    return txnPoolNodeSet
