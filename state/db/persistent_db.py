from state.db.db import BaseDB
from storage.kv_store import KeyValueStorage


class PersistentDB(BaseDB):
    def __init__(self, keyValueStorage: KeyValueStorage):
        self._keyValueStorage = keyValueStorage

    def get(self, key: bytes) -> bytes:
        return self._keyValueStorage.get(key)

    def _has_key(self, key: bytes):
        try:
            self.get(key)
            return True
        except KeyError:
            return False

    def __contains__(self, key):
        return self._has_key(key)

    def __eq__(self, other):
        is_k_eq = self._keyValueStorage == other._keyValueStorage
        return isinstance(other, self.__class__) and is_k_eq

    def inc_refcount(self, key, value):
        self._keyValueStorage.put(key, value)

    def dec_refcount(self, key):
        pass
