from copy import deepcopy
from random import random, randint, choice

import pytest

from plenum.common.util import randomString
from state.db.persistent_db import PersistentDB
from state.trie.pruning_trie import Trie, rlp_encode
from storage.kv_in_memory import KeyValueStorageInMemory


def gen_test_data(num_keys, max_key_size=64, max_val_size=256):
    return {randomString(max_key_size).encode():
            rlp_encode([randomString(max_val_size)]) for i in range(num_keys)}


def test_verify_proof():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    node_trie.update('k1'.encode(), rlp_encode(['v1']))
    node_trie.update('k2'.encode(), rlp_encode(['v2']))

    root_hash_0 = node_trie.root_hash
    p0 = node_trie.produce_spv_proof('k2'.encode())
    p0.append(deepcopy(node_trie.root_node))
    p00 = deepcopy(p0)
    assert client_trie.verify_spv_proof(root_hash_0, 'k2'.encode(),
                                        rlp_encode(['v2']), p0)
    assert p00 == p0

    node_trie.update('k3'.encode(), rlp_encode(['v3']))
    node_trie.update('k4'.encode(), rlp_encode(['v4']))
    node_trie.update('x1'.encode(), rlp_encode(['y1']))
    node_trie.update('x2'.encode(), rlp_encode(['y2']))
    root_hash_1 = node_trie.root_hash

    # Generate 1 proof and then verify that proof
    p1 = node_trie.produce_spv_proof('k1'.encode())
    p1.append(node_trie.root_node)
    assert client_trie.verify_spv_proof(root_hash_1, 'k1'.encode(),
                                        rlp_encode(['v1']), p1)

    p2 = node_trie.produce_spv_proof('x2'.encode())
    p2.append(node_trie.root_node)
    assert client_trie.verify_spv_proof(root_hash_1, 'x2'.encode(),
                                        rlp_encode(['y2']), p2)

    # Generate more than 1 proof and then verify all proofs

    p3 = node_trie.produce_spv_proof('k3'.encode())
    p3.append(node_trie.root_node)

    p4 = node_trie.produce_spv_proof('x1'.encode())
    p4.append(node_trie.root_node)

    assert client_trie.verify_spv_proof(root_hash_1, 'k3'.encode(),
                                        rlp_encode(['v3']), p3)
    assert client_trie.verify_spv_proof(root_hash_1, 'x1'.encode(),
                                        rlp_encode(['y1']), p4)

    # Proof is correct but value is different
    assert not client_trie.verify_spv_proof(root_hash_1, 'x1'.encode(),
                                            rlp_encode(['y99']), p4)

    # Verify same proof again
    assert client_trie.verify_spv_proof(root_hash_1, 'k3'.encode(),
                                        rlp_encode(['v3']), p3)

    assert p00 == p0
    assert client_trie.verify_spv_proof(root_hash_0, 'k2'.encode(),
                                        rlp_encode(['v2']), p0)


def test_verify_proof_generated_using_helper():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    node_trie.update('k1'.encode(), rlp_encode(['v1']))
    node_trie.update('k2'.encode(), rlp_encode(['v2']))

    root_hash_0 = node_trie.root_hash
    p0 = node_trie.generate_state_proof('k2'.encode())
    assert client_trie.verify_spv_proof(root_hash_0, 'k2'.encode(),
                                        rlp_encode(['v2']), p0)

    node_trie.update('k3'.encode(), rlp_encode(['v3']))
    node_trie.update('k4'.encode(), rlp_encode(['v4']))
    node_trie.update('x1'.encode(), rlp_encode(['y1']))
    node_trie.update('x2'.encode(), rlp_encode(['y2']))
    root_hash_1 = node_trie.root_hash

    # Generate 1 proof and then verify that proof
    p1 = node_trie.generate_state_proof('k1'.encode())
    assert client_trie.verify_spv_proof(root_hash_1, 'k1'.encode(),
                                        rlp_encode(['v1']), p1)

    p2 = node_trie.generate_state_proof('x2'.encode())
    assert client_trie.verify_spv_proof(root_hash_1, 'x2'.encode(),
                                        rlp_encode(['y2']), p2)

    # Generate more than 1 proof and then verify all proofs

    p3 = node_trie.generate_state_proof('k3'.encode())

    p4 = node_trie.generate_state_proof('x1'.encode())

    assert client_trie.verify_spv_proof(root_hash_1, 'k3'.encode(),
                                        rlp_encode(['v3']), p3)
    assert client_trie.verify_spv_proof(root_hash_1, 'x1'.encode(),
                                        rlp_encode(['y1']), p4)

    # Proof is correct but value is different
    assert not client_trie.verify_spv_proof(root_hash_1, 'x1'.encode(),
                                            rlp_encode(['y99']), p4)

    # Verify same proof again
    assert client_trie.verify_spv_proof(root_hash_1, 'k3'.encode(),
                                        rlp_encode(['v3']), p3)

    assert client_trie.verify_spv_proof(root_hash_0, 'k2'.encode(),
                                        rlp_encode(['v2']), p0)

    # Proof generated using non-existent key fails verification
    p5 = node_trie.generate_state_proof('x909'.encode())
    assert not client_trie.verify_spv_proof(root_hash_1, 'x909'.encode(),
                                            rlp_encode(['y909']), p5)


def test_verify_proof_random_data():
    """
    Add some key value pairs in trie. Generate and verify proof for them.
    :return:
    """
    num_keys = 100
    test_data = gen_test_data(num_keys)
    partitions = 4
    partition_size = num_keys // partitions
    keys = [list(list(test_data.keys())[i:i + partition_size])
            for i in range(0, len(test_data), partition_size)]

    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    root_hashes = []
    proofs = []
    for i in range(0, partitions):
        for k in keys[i]:
            node_trie.update(k, test_data[k])

        root_hashes.append(node_trie.root_hash)
        proofs.append({k: node_trie.generate_state_proof(k) for k in keys[i]})
        assert all([client_trie.verify_spv_proof(root_hashes[i],
                                                 k, test_data[k],
                                                 proofs[i][k]) for k in keys[i]])

    # Pick any keys from any partition and verify the already generated proof
    for _ in range(400):
        p = randint(0, partitions - 1)
        key = choice(keys[p])
        assert client_trie.verify_spv_proof(root_hashes[p], key,
                                            test_data[key], proofs[p][key])

    # Pick any key randomly, generate new proof corresponding to current root
    # and verify proof
    all_keys = [k for i in keys for k in i]
    root_hash = node_trie.root_hash
    for _ in range(400):
        key = choice(all_keys)
        proof = node_trie.generate_state_proof(key)
        assert client_trie.verify_spv_proof(root_hash, key,
                                            test_data[key], proof)


def test_proof_serialize_deserialize():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    keys = {k.encode(): [rlp_encode([v]), ] for k, v in
            [('k1', 'v1'), ('k2', 'v2'), ('k35', 'v55'), ('k70', 'v99')]}

    for k, v in keys.items():
        node_trie.update(k, v[0])

    for k in keys:
        keys[k].append(node_trie.generate_state_proof(k, serialize=True))

    for k in keys:
        prf = keys[k][1]
        assert isinstance(prf, bytes)
        assert client_trie.verify_spv_proof(node_trie.root_hash, k, keys[k][0],
                                            prf, serialized=True)


@pytest.mark.skip(reason='Need to check if need/possible to build proof for an '
                         'arbitrary old root')
def test_proof_specific_root():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    node_trie.update('k1'.encode(), rlp_encode(['v1']))
    node_trie.update('k2'.encode(), rlp_encode(['v2']))
    node_trie.update('x3'.encode(), rlp_encode(['v3']))

    root_hash_0 = node_trie.root_hash
    root_node_0 = node_trie.root_node

    node_trie.update('x4'.encode(), rlp_encode(['v5']))
    node_trie.update('y99'.encode(), rlp_encode(['v6']))
    node_trie.update('x5'.encode(), rlp_encode(['v7']))

    # root_hash_1 = node_trie.root_hash
    # root_node_1 = node_trie.root_node

    k, v = 'k1'.encode(), rlp_encode(['v1'])
    old_root_proof = node_trie.generate_state_proof(k, root=root_node_0)
    assert client_trie.verify_spv_proof(root_hash_0, k, v, old_root_proof)
