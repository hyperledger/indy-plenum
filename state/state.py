from abc import abstractmethod


class State:

    @abstractmethod
    def set(self, key: bytes, value: bytes):
        raise NotImplementedError

    @abstractmethod
    def get(self, key: bytes, isCommitted: bool=True):
        # If `isCommitted` is True then get value corresponding to the
        # committed state else get the latest value
        raise NotImplementedError

    @abstractmethod
    def remove(self, key: bytes):
        raise NotImplementedError

    @abstractmethod
    def commit(self, rootHash=None, rootNode=None):
        raise NotImplementedError

    @abstractmethod
    def revertToHead(self, headHash=None):
        # Revert to the given head
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def head(self):
        # The current head of the state, if the state is a merkle tree then
        # head is the root
        raise NotImplementedError

    @property
    @abstractmethod
    def committedHead(self):
        # The committed head of the state, if the state is a merkle tree then
        # head is the root
        raise NotImplementedError

    @property
    @abstractmethod
    def headHash(self):
        """
        The hash of the current head of the state, if the state is a merkle
        tree then hash of the root
        :return:
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def committedHeadHash(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def isEmpty(self):
        raise NotImplementedError
