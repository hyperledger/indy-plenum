from abc import abstractmethod, ABCMeta
from typing import Tuple, Iterable


class KeyValueStorage(metaclass=ABCMeta):

    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def remove(self, key):
        pass

    @abstractmethod
    def setBatch(self, batch: Iterable[Tuple]):
        pass

    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def iterator(self, start=None, end=None, include_key=True, include_value=True, prefix=None):
        pass

    @property
    @abstractmethod
    def closed(self):
        pass

    @property
    @abstractmethod
    def is_byte(self) -> bool:
        pass

    @property
    @abstractmethod
    def db_path(self) -> str:
        pass

    @property
    def size(self):
        c = 0
        for l in self.iterator():
            c += 1
        s = sum(1 for l in self.iterator())
        return sum(1 for l in self.iterator())

    def has_key(self, key):
        try:
            self.get(key)
            return True
        except KeyError:
            return False

    def __contains__(self, key):
        return self.has_key(key)

