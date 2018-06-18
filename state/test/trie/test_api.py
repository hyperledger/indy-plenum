import pytest

from common.exceptions import PlenumTypeError, PlenumValueError
from state.db.persistent_db import PersistentDB
from state.trie.pruning_trie import Trie
from storage.kv_in_memory import KeyValueStorageInMemory


def test_set_root_hash():
    trie = Trie(PersistentDB(KeyValueStorageInMemory()))

    for v in (None, [], ''):
        with pytest.raises(PlenumTypeError):
            trie.set_root_hash([])

    with pytest.raises(PlenumValueError):
        trie.set_root_hash(b'*' * 33)
