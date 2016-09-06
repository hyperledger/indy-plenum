"""
The secondary storage is expected to be able to:
1. store and retrieve Client transactions
2. store and retrieve previous replies

The data stored in the secondary storage may be a replication of
the primary storage's data but can be queried more effectively.
"""
from plenum.common.types import f


class SecondaryStorage:
    def __init__(self, txnStore, primaryStorage=None):
        self._txnStore = txnStore
        self._primaryStorage = primaryStorage

    def getReply(self, identifier, reqId, **kwargs):
        return self._primaryStorage.get(**{f.IDENTIFIER.nm: identifier,
                                           f.REQ_ID.nm: reqId})

    def getReplies(self, *txnIds, seqNo=None, **kwargs):
        raise NotImplementedError
