import pytest
import json

from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory
from ledger.compact_merkle_tree import CompactMerkleTree

from plenum.common.ledger import Ledger
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.testing_utils import FakeSomething

from plenum.test.txn_author_agreement.helper import (
    TaaData, expected_state_data
)

from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.testing_utils import FakeSomething
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from .helper import (
    sdk_send_txn_author_agreement, calc_taa_digest,
    gen_random_txn_author_agreement
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
def random_taa(request):
    marker = request.node.get_marker('random_taa')
    return gen_random_txn_author_agreement(
        **({} if marker is None else marker.kwargs))


@pytest.fixture
def taa_in_data():
    return [
        TaaData(*gen_random_txn_author_agreement(8, 32), n, n + 10)
        for n in range(10)
    ]


@pytest.fixture
def taa_state_data(taa_in_data):
    return {data.version: expected_state_data(data) for data in taa_in_data}


@pytest.fixture
def taa_digests(taa_in_data):
    return {data.version: calc_taa_digest(data.version, data.text) for data in taa_in_data}


@pytest.fixture(scope='module')
def set_txn_author_agreement(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):

    def wrapped(version=None, text=None):
        _random_taa = gen_random_txn_author_agreement()
        version = _random_taa[0] if version is None else version
        text = _random_taa[0] if text is None else text
        reply = sdk_send_txn_author_agreement(
            looper, sdk_pool_handle, sdk_wallet_trustee, version, text)[0]
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
        return reply

    return wrapped
