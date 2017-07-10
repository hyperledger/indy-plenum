from typing import Tuple, Iterable

from rlp.utils import str_to_bytes
from state.kv.kv_store import KeyValueStorage
from state.util import utils

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

    def open(self):
        pass

    def close(self):
        pass

    def drop(self):
        self._dict = {}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._dict == other._dict

    def __hash__(self):
        return utils.big_endian_to_int(str_to_bytes(self.__repr__()))
