import pytest

from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory
from ledger.compact_merkle_tree import CompactMerkleTree

from plenum.common.util import randomString
from plenum.common.ledger import Ledger
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.testing_utils import FakeSomething

from plenum.test.txn_author_agreement.helper import (
    TaaData, expected_state_data
)


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
                            domain_state=FakeSomething())


@pytest.fixture
def taa_in_data():
    return [
        TaaData(randomString(8), randomString(32), n, n + 10)
        for n in range(10)
    ]


@pytest.fixture
def taa_state_data(taa_in_data):
    return {data.version: expected_state_data(data) for data in taa_in_data}


@pytest.fixture
def taa_digests(taa_in_data):
    # TODO use some other API, e.g. sdk's one
    return {data.version: ConfigReqHandler._taa_digest(
        data.version, data.text).decode() for data in taa_in_data}
