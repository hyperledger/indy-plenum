class ImmutableStore:
    """
    Interface for immutable stores.
    An immutable store is any storage system (database, flatfile, in-memory,
    etc.). It stores the transaction data and the relevant info from the
    Merkle Tree.
    """

    def start(self, loop):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()

    def append(self, reply):
        raise NotImplementedError()

    def get(self, identifier: str, reqId: int):
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()
