import random
from copy import deepcopy

import pytest

from state.db.persistent_db import PersistentDB
from state.pruning_state import PruningState
from state.state import State
from state.trie.pruning_trie import bin_to_nibbles, rlp_decode, Trie, rlp_encode
from state.util.utils import sha3
from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.kv_store_rocksdb import KeyValueStorageRocksdb


@pytest.yield_fixture(params=['memory', 'leveldb', 'rocksdb'])
def state(request, tmpdir_factory) -> State:
    if request.param == 'leveldb':
        db = KeyValueStorageLeveldb(tmpdir_factory.mktemp('').strpath,
                                    'some_db')
    elif request.param == 'rocksdb':
        db = KeyValueStorageRocksdb(tmpdir_factory.mktemp('').strpath,
                                    'some_db')
    else:
        db = KeyValueStorageInMemory()
    state = PruningState(db)
    yield state
    state.close()


def test_state_proof_and_verification(state):
    state.set(b'k1', b'v1')
    state.set(b'k2', b'v2')
    state.set(b'k3', b'v3')
    state.set(b'k4', b'v4')

    p1 = state.generate_state_proof(b'k1')
    p2 = state.generate_state_proof(b'k2')
    p3 = state.generate_state_proof(b'k3')
    p4 = state.generate_state_proof(b'k4')

    # Verify correct proofs and values
    assert PruningState.verify_state_proof(state.headHash, b'k1', b'v1', p1)
    assert PruningState.verify_state_proof(state.headHash, b'k2', b'v2', p2)
    assert PruningState.verify_state_proof(state.headHash, b'k3', b'v3', p3)
    assert PruningState.verify_state_proof(state.headHash, b'k4', b'v4', p4)

    # Incorrect proof
    assert PruningState.verify_state_proof(state.headHash, b'k3', b'v3', p4)

    # Correct proof but incorrect value
    assert not PruningState.verify_state_proof(state.headHash, b'k2', b'v1', p2)
    assert not PruningState.verify_state_proof(state.headHash, b'k4', b'v2', p4)


def test_state_proof_and_verification_serialized(state):
    data = {k.encode(): v.encode() for k, v in
            [('k1', 'v1'), ('k2', 'v2'), ('k35', 'v55'), ('k70', 'v99')]}

    for k, v in data.items():
        state.set(k, v)

    proofs = {k: state.generate_state_proof(k, serialize=True) for k in data}

    for k, v in data.items():
        assert PruningState.verify_state_proof(state.headHash, k, v,
                                               proofs[k], serialized=True)


def test_state_proof_for_missing_data(state):
    p1 = state.generate_state_proof(b'k1')
    assert PruningState.verify_state_proof(state.headHash, b'k1', None, p1)


def test_state_proof_for_key_prefix(state):
    prefix = 'abcdefgh'
    keys_suffices = [1, 4, 10, 11, 24, 99, 100]
    key_vals = {'{}{}'.format(prefix, k): str(random.randint(3000, 5000))
                for k in keys_suffices}
    for k, v in key_vals.items():
        state.set(k.encode(), v.encode())
        prefix_prf = state.generate_state_proof(prefix.encode())

    prefix_prf = state.generate_state_proof(prefix.encode())
    assert PruningState.verify_state_proof(state.headHash, prefix.encode(),
                                           None, prefix_prf)


def test_state_proof_for_key_prefix_1(state):
    prefix = 'abcdefgh'
    state.set(prefix.encode(), b'2122')
    keys_suffices = [1, 4, 10, 11, 24, 99, 100]
    key_vals = {'{}{}'.format(prefix, k): str(random.randint(3000, 5000))
                for k in keys_suffices}
    for k, v in key_vals.items():
        state.set(k.encode(), v.encode())
        prefix_prf = state.generate_state_proof(prefix.encode())

    prefix_prf = state.generate_state_proof(prefix.encode())
    assert PruningState.verify_state_proof(state.headHash, prefix.encode(),
                                           None, prefix_prf)


def test_state_proof_for_key_prefix_2(state):
    prefix = 'abcdefgh'
    state.set((prefix[:4]+'zzzz').encode(), b'1908')
    keys_suffices = [1, 4, 10, 11, 24, 99, 100]
    key_vals = {'{}{}'.format(prefix, k): str(random.randint(3000, 5000))
                for k in keys_suffices}
    for k, v in key_vals.items():
        state.set(k.encode(), v.encode())
        prefix_prf = state.generate_state_proof(prefix.encode())

    prefix_prf = state.generate_state_proof(prefix.encode())
    assert PruningState.verify_state_proof(state.headHash, prefix.encode(),
                                           None, prefix_prf)


def test_state_proof_for_key_prefix_3(state, tmpdir_factory):
    trie = state._trie
    prefix = 'abcdefgh'
    state.set(b'zyxwvuts', b'1115')
    state.set(b'rqponmlk', b'0989')
    keys_suffices = [1, 4, 10, 11, 24, 99, 100]
    key_vals = {'{}{}'.format(prefix, k): str(random.randint(3000, 5000))
                for k in keys_suffices}
    nib = bin_to_nibbles(prefix.encode())
    for k, v in key_vals.items():
        state.set(k.encode(), v.encode())
        prefix_node = trie._get_last_node_for_prfx(trie.root_node, nib)
        print('prefix node type ', k, trie._get_node_type(prefix_node))
        val = trie._get(prefix_node, bin_to_nibbles(k.encode())[1:])
        val = rlp_decode(val)
        assert val[0] == v.encode()

    print('all proofs')
    prefix_node = trie._get_last_node_for_prfx(trie.root_node, nib)
    print('prefix node type ', trie._get_node_type(prefix_node))
    for k, v in key_vals.items():
        val = trie._get(prefix_node, bin_to_nibbles(k.encode())[1:])
        val = rlp_decode(val)
        assert val[0] == v.encode()
        print(trie.produce_spv_proof(k.encode(), trie.root_node))

    prefix_prf = trie.produce_spv_proof_for_key_prfx(prefix.encode(), trie.root_node)
    print(prefix_prf)
    prefix_prf.append(deepcopy(trie.root_node))
    # _db = KeyValueStorageLeveldb(tmpdir_factory.mktemp('').strpath, 'temp_db')
    _db = KeyValueStorageInMemory()

    for node in prefix_prf:
        R = rlp_encode(node)
        H = sha3(R)
        _db.put(H, R)

    new_trie = Trie(PersistentDB(_db))

    print(trie._get_node_type(trie.root_node))
    root = state.headHash

    for k, v in key_vals.items():
        v = rlp_encode(v.encode())
        new_trie.update(k.encode(), v)

    new_trie.root_hash = root
    for k, v in key_vals.items():
        _v = new_trie.get(k.encode())
        assert v.encode() == rlp_decode(_v)[0]


def test_state_proof_for_key_prefix_4(state):
    trie = state._trie
    prefix = 'abcdefgh'
    state.set(b'zyxwvuts', b'1115')
    state.set(b'rqponmlk', b'0989')
    # More than 16
    keys_suffices = {random.randint(150, 900) for _ in range(100)}
    key_vals = {'{}{}'.format(prefix, k): str(random.randint(3000, 5000))
                for k in keys_suffices}
    for k, v in key_vals.items():
        state.set(k.encode(), v.encode())
        nib = bin_to_nibbles(prefix.encode())
        prefix_node = trie._get_last_node_for_prfx(trie.root_node, nib)
        print('prefix node type ', k, trie._get_node_type(prefix_node))
        val = trie._get(prefix_node, bin_to_nibbles(k.encode())[1:])
        val = rlp_decode(val)
        assert val[0] == v.encode()
        prefix_prf = trie.produce_spv_proof_for_key_prfx(prefix.encode(),
                                                         trie.root_node)

    prefix_prf = state.generate_state_proof(prefix.encode())
    assert PruningState.verify_state_proof(state.headHash, prefix.encode(),
                                           None, prefix_prf)

    print('all proofs')
    for k, v in key_vals.items():
        print(trie.produce_spv_proof(k.encode(), trie.root_node))
