from typing import Tuple, Iterable

from rlp.utils import str_to_bytes
from state.util import utils
from storage.kv_store import KeyValueStorage

# log = get_logger('db')


databases = {}


class KeyValueStorageInMemory(KeyValueStorage):
    def __init__(self):
        self._dict = {}

    def get(self, key):
        if isinstance(key, str):
            key = key.encode()
        return self._dict[key]

    def put(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()
        self._dict[key] = value

    def remove(self, key):
        if isinstance(key, str):
            key = key.encode()
        del self._dict[key]

    def setBatch(self, batch: Iterable[Tuple]):
        for key, value in batch:
            self.put(key, value)

    def do_ops_in_batch(self, batch: Iterable[Tuple]):
        for op, key, value in batch:
            if op == self.WRITE_OP:
                self.put(key, value)
            elif op == self.REMOVE_OP:
                self.remove(key)
            else:
                raise ValueError('Unknown operation')

    def open(self):
        pass

    def close(self):
        pass

    def drop(self):
        self._dict = {}

    def reset(self):
        self._dict = {}

    def iterator(self, start=None, end=None, include_key=True, include_value=True, prefix=None):
        if not (include_key or include_value):
            raise ValueError("At least one of includeKey or includeValue "
                             "should be true")

        def filter(key, start, end):
            if start and end:
                return key in range(start, end)
            if start:
                return key >= start
            if end:
                return key <= end

        if include_key and include_value:
            if start or end:
                return {k: v for k, v in self._dict.items() if filter(k, start, end)}
            return self._dict.items()
        if include_key:
            if start or end:
                return (k for k in self._dict.keys() if filter(k, start, end))
            return self._dict.keys()
        if include_value:
            if start or end:
                return (v for k, v in self._dict.items() if filter(k, start, end))
            return self._dict.values()

    def closed(self):
        return False

    def is_byte(self):
        return False

    def db_path(self) -> str:
        return ""

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._dict == other._dict

    def __hash__(self):
        return utils.big_endian_to_int(str_to_bytes(self.__repr__()))
