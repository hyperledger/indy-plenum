from typing import Iterable, Tuple

from storage.kv_store import KeyValueStorage


# TODO: WIP below


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
