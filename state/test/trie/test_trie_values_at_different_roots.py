from state.db.persistent_db import PersistentDB
from state.trie.pruning_trie import BLANK_NODE, Trie
from state.util.fast_rlp import encode_optimized as rlp_encode, \
    decode_optimized as rlp_decode
from storage.kv_in_memory import KeyValueStorageInMemory


def test_get_values_at_roots_in_memory():
    # Update key with different values but preserve root after each update
    # Check values of keys with different previous roots and check that they
    # are correct
    trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    trie.update('k1'.encode(), rlp_encode(['v1']))
    # print state.root_hash.encode('hex')
    # print state.root_node

    val = trie.get('k1')
    print(rlp_decode(val))
    oldroot1 = trie.root_node
    old_root1_hash = trie.root_hash
    assert trie._decode_to_node(old_root1_hash) == oldroot1
    trie.update('k1'.encode(), rlp_encode(['v1a']))
    val = trie.get('k1')
    assert rlp_decode(val) == [b'v1a', ]

    # Already saved roots help in getting previous values
    oldval = trie.get_at(oldroot1, 'k1')
    assert rlp_decode(oldval) == [b'v1', ]
    oldroot1a = trie.root_node

    trie.update('k1'.encode(), rlp_encode([b'v1b']))
    val = trie.get('k1')
    assert rlp_decode(val) == [b'v1b']

    oldval = trie.get_at(oldroot1a, 'k1')
    assert rlp_decode(oldval) == [b'v1a', ]

    oldval = trie.get_at(oldroot1, 'k1')
    assert rlp_decode(oldval) == [b'v1', ]

    oldroot1b = trie.root_node

    trie.update('k1'.encode(), rlp_encode([b'v1c']))
    val = trie.get('k1')
    assert rlp_decode(val) == [b'v1c', ]

    oldval = trie.get_at(oldroot1b, 'k1')
    assert rlp_decode(oldval) == [b'v1b', ]

    oldval = trie.get_at(oldroot1a, 'k1')
    assert rlp_decode(oldval) == [b'v1a', ]

    oldval = trie.get_at(oldroot1, 'k1')
    assert rlp_decode(oldval) == [b'v1', ]

    oldroot1c = trie.root_node

    trie.delete('k1'.encode())
    assert trie.get('k1') == BLANK_NODE

    oldval = trie.get_at(oldroot1c, 'k1')
    assert rlp_decode(oldval) == [b'v1c', ]

    oldval = trie.get_at(oldroot1b, 'k1')
    assert rlp_decode(oldval) == [b'v1b', ]

    oldval = trie.get_at(oldroot1a, 'k1')
    assert rlp_decode(oldval) == [b'v1a', ]

    oldval = trie.get_at(oldroot1, 'k1')
    assert rlp_decode(oldval) == [b'v1', ]

    trie.root_node = oldroot1c
    val = trie.get('k1')
    assert rlp_decode(val) == [b'v1c', ]

    trie.root_node = oldroot1
    val = trie.get('k1')
    assert rlp_decode(val) == [b'v1', ]
