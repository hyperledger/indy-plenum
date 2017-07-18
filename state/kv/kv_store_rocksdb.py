from typing import Iterable, Tuple
from state.kv.kv_store import KeyValueStorage

try:
    import rocksdb
    from rocksdb.interfaces import Comparator
except ImportError:
    print('Cannot import rocksdb, please install')

# TODO: WIP below

class IntegerComparator(Comparator):
    def compare(self, a, b):
        a = int(a)
        b = int(b)
        if (a < b):
            return -1
        if (a > b):
            return 1
        if (a == b):
            return 0

    def name(self):
        return b'IntegerComparator'

class KeyValueStorageRocksdb(KeyValueStorage):
    def set(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def remove(self, key):
        raise NotImplementedError

    def setBatch(self, batch: Iterable[Tuple]):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
