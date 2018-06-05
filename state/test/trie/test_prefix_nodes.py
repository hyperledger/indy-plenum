from state.db.persistent_db import PersistentDB
from state.trie.pruning_trie import Trie, rlp_encode, bin_to_nibbles, \
    NODE_TYPE_LEAF, NODE_TYPE_EXTENSION, unpack_to_nibbles, \
    without_terminator, BLANK_NODE, NODE_TYPE_BRANCH, starts_with
from storage.kv_in_memory import KeyValueStorageInMemory


def test_get_prefix_nodes():
    trie = Trie(PersistentDB(KeyValueStorageInMemory()))
    prefix = 'abcd'
    prefix_nibbles = bin_to_nibbles(prefix)
    key1 = prefix + '1'
    key2 = prefix + '2'
    key3 = prefix + '3'
    trie.update(key1.encode(), rlp_encode(['v1']))
    seen_prefix = []
    last_node = trie._get_last_node_for_prfx(trie.root_node, prefix_nibbles,
                                             seen_prfx=seen_prefix)
    # The last node should be a leaf since only 1 key
    assert trie._get_node_type(last_node) == NODE_TYPE_LEAF
    # Seen prefix matches the prefix exactly
    assert seen_prefix == []

    # The queried key is larger than prefix, results in blank node
    last_node_ = trie._get_last_node_for_prfx(trie.root_node,
                                              bin_to_nibbles(prefix + '5'), [])
    assert last_node_ == BLANK_NODE

    seen_prefix = []
    trie.update(key2.encode(), rlp_encode(['v2']))
    last_node = trie._get_last_node_for_prfx(trie.root_node, prefix_nibbles,
                                             seen_prfx=seen_prefix)
    # The last node should be an extension since more than 1 key
    assert trie._get_node_type(last_node) == NODE_TYPE_EXTENSION
    assert seen_prefix == []

    seen_prefix = []
    trie.update(key3.encode(), rlp_encode(['v3']))
    last_node = trie._get_last_node_for_prfx(trie.root_node, prefix_nibbles,
                                             seen_prfx=seen_prefix)
    assert trie._get_node_type(last_node) == NODE_TYPE_EXTENSION
    assert seen_prefix == []

    last_node_key = without_terminator(unpack_to_nibbles(last_node[0]))
    # Key for the fetched prefix nodes (ignore last nibble) is same as prefix nibbles
    assert last_node_key[:-1] == prefix_nibbles

    # The extension node is correctly decoded.
    decoded_extension = trie._decode_to_node(last_node[1])
    assert decoded_extension[1] == [b' ', rlp_encode(['v1'])]
    assert decoded_extension[2] == [b' ', rlp_encode(['v2'])]
    assert decoded_extension[3] == [b' ', rlp_encode(['v3'])]

    # Add keys with extended prefix
    extended_prefix = '1'
    key4 = prefix + extended_prefix + '85'
    trie.update(key4.encode(), rlp_encode(['v11']))
    key5 = prefix + extended_prefix + '96'
    trie.update(key5.encode(), rlp_encode(['v12']))
    seen_prefix = []
    new_prefix_nibbs = bin_to_nibbles(prefix + extended_prefix)
    last_node = trie._get_last_node_for_prfx(trie.root_node, new_prefix_nibbs,
                                             seen_prfx=seen_prefix)

    assert trie._get_node_type(last_node) == NODE_TYPE_BRANCH
    assert new_prefix_nibbs == seen_prefix
    assert seen_prefix == bin_to_nibbles(prefix + '1')

    # traverse to the next node
    remaining_key4_nibbs = bin_to_nibbles(key4)[len(seen_prefix):]
    remaining_key5_nibbs = bin_to_nibbles(key5)[len(seen_prefix):]
    next_nibble = remaining_key4_nibbs[0] if remaining_key4_nibbs[0] > remaining_key5_nibbs[0] else remaining_key5_nibbs[0]
    next_node = trie._decode_to_node(last_node[next_nibble])

    assert trie._get_node_type(next_node) == NODE_TYPE_BRANCH

    # The 8th index should lead to a node with key '5', key4 ended in '85'
    assert trie._get_node_type(next_node[8]) == NODE_TYPE_LEAF
    assert without_terminator(unpack_to_nibbles(next_node[8][0])) == bin_to_nibbles('5')

    # The 9th index should lead to a node with key '6', key5 ended in '96'
    assert trie._get_node_type(next_node[9]) == NODE_TYPE_LEAF
    assert without_terminator(unpack_to_nibbles(next_node[9][0])) == bin_to_nibbles('6')

    prefix_1 = prefix + 'efgh'
    prefix_1_nibbles = bin_to_nibbles(prefix_1)
    key1 = prefix_1 + '1'
    key2 = prefix_1 + '2'
    key3 = prefix_1 + '3'

    trie.update(key1.encode(), rlp_encode(['v1']))
    trie.update(key2.encode(), rlp_encode(['v1']))
    trie.update(key3.encode(), rlp_encode(['v1']))

    seen_prefix = []
    last_node = trie._get_last_node_for_prfx(trie.root_node, prefix_1_nibbles,
                                             seen_prfx=seen_prefix)
    assert trie._get_node_type(last_node) == NODE_TYPE_EXTENSION
    assert len(seen_prefix) > 0
    assert starts_with(prefix_1_nibbles, seen_prefix)
