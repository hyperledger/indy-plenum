from abc import abstractmethod
from typing import Tuple, Iterable


class KeyValueStorage:

    @abstractmethod
    def put(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError

    @abstractmethod
    def remove(self, key):
        raise NotImplementedError

    @abstractmethod
    def setBatch(self, batch: Iterable[Tuple]):
        raise NotImplementedError

    @abstractmethod
    def open(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def drop(self):
        raise NotImplementedError

    def has_key(self, key):
        try:
            self.get(key)
            return True
        except KeyError:
            return False

    def __contains__(self, key):
        return self.has_key(key)

    @property
    def closed(self):
        return self._db is None

    @property
    def size(self):
        c = 0
        for _ in self.iter(include_value=False):
            c += 1
        return c

    def __repr__(self):
        return self._dbPath
