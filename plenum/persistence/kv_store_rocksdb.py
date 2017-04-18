from typing import Iterable, Tuple


# TODO: WIP below
class KVStoreRocksdb:
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
