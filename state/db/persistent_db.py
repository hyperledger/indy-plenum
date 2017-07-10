from state.db.db import BaseDB
from state.kv.kv_store import KeyValueStorage


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
        return isinstance(other, self.__class__) and self._keyValueStorage == other._keyValueStorage

    def inc_refcount(self, key, value):
        self._keyValueStorage.put(key, value)

    def dec_refcount(self, key):
        pass
