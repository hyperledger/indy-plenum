import json
from _sha256 import sha256

import pytest

from ledger.compact_merkle_tree import CompactMerkleTree
from plenum.common.constants import TXN_AUTHOR_AGREEMENT_VERSION, TXN_AUTHOR_AGREEMENT_TEXT
from plenum.common.ledger import Ledger
from plenum.common.txn_util import reqToTxn
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.test.testing_utils import FakeSomething
from plenum.test.txn_author_agreement.helper import gen_txn_author_agreement
from state.pruning_state import PruningState
from storage.kv_in_memory import KeyValueStorageInMemory


@pytest.fixture
def config_state():
    return PruningState(KeyValueStorageInMemory())


@pytest.fixture
def config_ledger(tmpdir_factory):
    tdir = tmpdir_factory.mktemp('').strpath
    return Ledger(CompactMerkleTree(),
                  dataDir=tdir)


@pytest.fixture
def config_req_handler(config_state,
                       config_ledger):

    return ConfigReqHandler(config_ledger,
                            config_state,
                            domain_state=FakeSomething())


def check_state_contains_taa(state, version: str, text: str, txn_hash: str):
    taa = state.get(':taa:h:{}'.format(txn_hash).encode(), isCommitted=False)
    assert taa is not None

    taa = json.loads(taa.decode())
    assert taa[TXN_AUTHOR_AGREEMENT_VERSION] == version
    assert taa[TXN_AUTHOR_AGREEMENT_TEXT] == text


def test_txn_author_agreement_updates_state(config_req_handler, sdk_wallet_trustee):
    version = 'some_version'
    text = 'some text'
    req = gen_txn_author_agreement(sdk_wallet_trustee[1], version=version, text=text)
    txn = reqToTxn(req)

    state = config_req_handler.state
    state_hash_before = state.headHash
    config_req_handler.updateState([txn])

    txn_digest = config_req_handler._taa_digest(version, text)

    assert state.headHash != state_hash_before
    assert config_req_handler.get_taa_digest(isCommitted=False) == txn_digest.encode()
    assert config_req_handler.get_taa_digest(version=version, isCommitted=False) == txn_digest.encode()

    taa = state.get(config_req_handler._state_path_taa_digest(txn_digest), isCommitted=False)
    assert taa is not None

    taa = json.loads(taa.decode())
    assert taa[TXN_AUTHOR_AGREEMENT_VERSION] == version
    assert taa[TXN_AUTHOR_AGREEMENT_TEXT] == text
