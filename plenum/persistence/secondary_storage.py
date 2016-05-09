"""
The secondary storage is expected to be able to:
1. store and retrieve Client transactions
2. store and retrieve previous replies

The data stored in the secondary storage may be a replication of
the primary storage's data but can be queried more effectively.
"""


class SecondaryStorage:
    def __init__(self, txnStore, primaryStorage=None):
        self._txnStore = txnStore
        self._primaryStorage = primaryStorage

    async def getReply(self, identifier, reqId, **kwargs):
        return await self._primaryStorage.get(identifier, reqId)

    def getReplies(self, *txnIds, seqNo=None, **kwargs):
        raise NotImplementedError
