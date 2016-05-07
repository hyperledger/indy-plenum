"""
The secondary storage is expected to have the following:
1. An implementation of HashStore for Merkle Proofs
2. A method to retrieve Client transactions
3. A method to retrieve previous replies

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
