from collections import OrderedDict
from copy import deepcopy
from random import randint, choice

from plenum.common.util import randomString
from state.db.persistent_db import PersistentDB
from state.trie.pruning_trie import Trie, rlp_encode, rlp_decode
from storage.kv_in_memory import KeyValueStorageInMemory


def gen_test_data(num_keys, max_key_size=64, max_val_size=256):
    return {randomString(max_key_size).encode():
            rlp_encode([randomString(max_val_size)]) for _ in range(num_keys)}


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


def test_proof_specific_root():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    kvs = OrderedDict({'k1': 'v1', 'k2': 'v2', 'x3': 'v3', 'x4': 'v5',
                       'x5': 'v7', 'y99': 'v6'})
    size = len(kvs)

    # Only add some keys
    old_keys = set()
    i = 0
    for k, v in kvs.items():
        node_trie.update(k.encode(), rlp_encode([v]))
        old_keys.add(k)
        i += 1
        if i >= size // 2:
            break

    # Record the root
    root_hash_0 = node_trie.root_hash
    root_node_0 = deepcopy(node_trie.root_node)

    # Add remaining keys
    new_keys = set()
    i = 0
    for k, v in reversed(kvs.items()):
        node_trie.update(k.encode(), rlp_encode([v]))
        new_keys.add(k)
        i += 1
        if i >= size // 2:
            break

    # Record new roots
    root_hash_1 = node_trie.root_hash
    root_node_1 = deepcopy(node_trie.root_node)

    # Check each root present
    for k, v in kvs.items():
        assert node_trie.get(k.encode()) == rlp_encode([v])

    # Old and new roots should be different
    assert root_hash_0 != root_hash_1
    assert root_node_0 != root_node_1

    # Generate and verify proof for both old (if key was present) and new roots
    for k, v in kvs.items():
        k, v = k.encode(), rlp_encode([v])

        if k in old_keys:
            old_root_proof = node_trie.generate_state_proof(k, root=root_node_0)
            assert client_trie.verify_spv_proof(root_hash_0, k, v, old_root_proof)

        new_root_proof = node_trie.generate_state_proof(k, root=root_node_1)
        assert client_trie.verify_spv_proof(root_hash_1, k, v, new_root_proof)


def test_proof_prefix_only_prefix_nodes():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    prefix = 'abcdefgh'
    keys_suffices = set()
    while len(keys_suffices) != 20:
        keys_suffices.add(randint(25, 25000))

    key_vals = {'{}{}'.format(prefix, k): str(randint(3000, 5000))
                for k in keys_suffices}
    for k, v in key_vals.items():
        node_trie.update(k.encode(), rlp_encode([v]))

    proof_nodes, val = node_trie.generate_state_proof_for_keys_with_prefix(
        prefix.encode(), get_value=True)
    encoded = {k.encode(): rlp_encode([v]) for k, v in key_vals.items()}
    # Check returned values match the actual values
    assert encoded == val
    assert client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                              encoded, proof_nodes)
    # Check without value
    proof_nodes = node_trie.generate_state_proof_for_keys_with_prefix(
        prefix.encode(), get_value=False)
    assert client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                              encoded, proof_nodes)


def test_proof_prefix_with_other_nodes():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    prefix = 'abcdefgh'

    other_nodes_count = 1000
    prefix_nodes_count = 100

    # Some nodes before prefix node
    for _ in range(other_nodes_count):
        node_trie.update(randomString(randint(8, 19)).encode(),
                         rlp_encode([randomString(15)]))

    keys_suffices = set()
    while len(keys_suffices) != prefix_nodes_count:
        keys_suffices.add(randint(25, 250000))

    key_vals = {'{}{}'.format(prefix, k): str(randint(3000, 5000))
                for k in keys_suffices}
    for k, v in key_vals.items():
        node_trie.update(k.encode(), rlp_encode([v]))

    # Some nodes after prefix node
    for _ in range(other_nodes_count):
        node_trie.update(randomString(randint(8, 19)).encode(),
                         rlp_encode([randomString(15)]))

    proof_nodes, val = node_trie.generate_state_proof_for_keys_with_prefix(prefix.encode(), get_value=True)
    encoded = {k.encode(): rlp_encode([v]) for k, v in key_vals.items()}
    # Check returned values match the actual values
    assert encoded == val
    assert client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                              encoded, proof_nodes)
    # Check without value
    proof_nodes = node_trie.generate_state_proof_for_keys_with_prefix(
        prefix.encode(), get_value=False)
    assert client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                              encoded, proof_nodes)

    # Change value of one of any random key
    encoded_new = deepcopy(encoded)
    random_key = next(iter(encoded_new.keys()))
    encoded_new[random_key] = rlp_encode([rlp_decode(encoded_new[random_key])[0] + b'2212'])
    assert not client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                                  encoded_new, proof_nodes)


def test_proof_multiple_prefix_nodes():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    prefix_1 = 'abcdefgh'
    prefix_2 = 'abcdefxy'  # Prefix overlaps with previous
    prefix_3 = 'pqrstuvw'
    prefix_4 = 'mnoptuvw'  # Suffix overlaps

    all_prefixes = (prefix_1, prefix_2, prefix_3, prefix_4)

    other_nodes_count = 1000
    prefix_nodes_count = 100

    # Some nodes before prefix nodes
    for _ in range(other_nodes_count):
        k, v = randomString(randint(8, 19)).encode(), rlp_encode([randomString(15)])
        node_trie.update(k, v)

    keys_suffices = set()
    while len(keys_suffices) != prefix_nodes_count:
        keys_suffices.add(randint(25, 250000))

    key_vals = {'{}{}'.format(prefix, k): str(randint(3000, 5000))
                for prefix in all_prefixes for k in keys_suffices}
    for k, v in key_vals.items():
        node_trie.update(k.encode(), rlp_encode([v]))

    # Some nodes after prefix nodes
    for _ in range(other_nodes_count):
        node_trie.update(randomString(randint(8, 19)).encode(),
                         rlp_encode([randomString(15)]))

    for prefix in all_prefixes:
        client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
        proof_nodes, val = node_trie.generate_state_proof_for_keys_with_prefix(
            prefix.encode(), get_value=True)
        encoded = {k.encode(): rlp_encode([v]) for k, v in key_vals.items() if k.startswith(prefix)}
        # Check returned values match the actual values
        assert encoded == val
        assert client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                                  encoded, proof_nodes)
        # Check without value
        proof_nodes = node_trie.generate_state_proof_for_keys_with_prefix(
            prefix.encode(), get_value=False)
        assert client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                                  encoded, proof_nodes)

        # Verify keys with a different prefix
        encoded = {k.encode(): rlp_encode([v]) for k, v in key_vals.items() if
                   not k.startswith(prefix)}
        assert not client_trie.verify_spv_proof_multi(node_trie.root_hash,
                                                      encoded, proof_nodes)


def test_get_proof_and_value():
    # Non prefix nodes
    num_keys = 100
    test_data = gen_test_data(num_keys)

    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    client_trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    for k, v in test_data.items():
        node_trie.update(k, v)

    for k in test_data:
        proof, v = node_trie.produce_spv_proof(k, get_value=True)
        proof.append(deepcopy(node_trie.root_node))
        assert v == test_data[k]
        assert client_trie.verify_spv_proof(node_trie.root_hash, k, v, proof)


def test_get_proof_and_value_no_key():
    node_trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    assert ([], None) == node_trie.produce_spv_proof(b"unknown_key", get_value=True)
