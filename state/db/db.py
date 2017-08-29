from abc import abstractmethod


class BaseDB:

    @abstractmethod
    def inc_refcount(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def dec_refcount(self, key):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError
