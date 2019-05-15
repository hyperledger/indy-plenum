import pytest
import json

from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory
from ledger.compact_merkle_tree import CompactMerkleTree

from plenum.common.ledger import Ledger
from plenum.server.config_req_handler import ConfigReqHandler

from plenum.test.txn_author_agreement.helper import (
    TaaData, expected_state_data, expected_data
)

from plenum.test.helper import sdk_get_and_check_replies
from plenum.test.testing_utils import FakeSomething
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from plenum.test.pool_transactions.helper import sdk_sign_and_send_prepared_request
from .helper import (
    sdk_send_txn_author_agreement, calc_taa_digest,
    gen_random_txn_author_agreement, get_config_req_handler
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
def taa_input_data():
    return [
        TaaData(*gen_random_txn_author_agreement(32, 8), n, n + 10)
        for n in range(10)
    ]


@pytest.fixture
def taa_expected_state_data(taa_input_data):
    return {data.version: expected_state_data(data) for data in taa_input_data}


@pytest.fixture
def taa_expected_data(taa_input_data):
    return {data.version: expected_data(data) for data in taa_input_data}


@pytest.fixture
def taa_expected_digests(taa_input_data):
    return {data.version: calc_taa_digest(data.text, data.version) for data in taa_input_data}


@pytest.fixture(scope='module')
def set_txn_author_agreement(looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_trustee):

    def wrapped(text=None, version=None):
        _random_taa = gen_random_txn_author_agreement()
        text = _random_taa[0] if text is None else text
        version = _random_taa[1] if version is None else version
        reply = sdk_send_txn_author_agreement(
            looper, sdk_pool_handle, sdk_wallet_trustee, text, version)[0]
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
        return reply

    return wrapped


# TODO: Replace implementation with get transaction
@pytest.fixture(scope='module')
def get_txn_author_agreement(txnPoolNodeSet):

    def wrapped(node=None, version=None, digest=None):
        node = txnPoolNodeSet[0] if node is None else node
        config_req_handler = get_config_req_handler(node)

        taa_digest = config_req_handler.get_taa_digest(version=version) if digest is None else digest
        taa_data = config_req_handler.get_taa_data(digest=digest, version=version)
        if taa_data:
            taa_data = TaaData(**taa_data[0], seq_no=taa_data[1], txn_time=taa_data[2])

        return taa_data, taa_digest

    return wrapped


@pytest.fixture
def latest_taa(get_txn_author_agreement):
    data, digest = get_txn_author_agreement()
    return {
        'data': data,
        'digest': digest
    }


@pytest.fixture
def activate_taa(set_txn_author_agreement):
    set_txn_author_agreement()
