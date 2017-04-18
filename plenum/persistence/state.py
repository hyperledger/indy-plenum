class State:
    def __init__(self, db, initState):
        raise NotImplementedError

    def set(self, key: bytes, value: bytes):
        raise NotImplementedError

    def get(self, key: bytes, isCommitted: bool=True):
        # If `isCommitted` is True then get value corresponding to the
        # committed state else get the latest value
        raise NotImplementedError

    def remove(self, key: bytes):
        raise NotImplementedError

    @property
    def head(self):
        # The current head of the state, if the state is a merkle tree then
        # head is the root
        raise NotImplementedError

    @property
    def committedHead(self):
        # The committed head of the state, if the state is a merkle tree then
        # head is the root
        raise NotImplementedError

    def revertToCommittedHead(self):
        # Make the current head same as the committed head
        raise NotImplementedError
