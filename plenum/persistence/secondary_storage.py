"""
The secondary storage is expected to be able to:
1. store and retrieve Client transactions
2. store and retrieve previous replies

The data stored in the secondary storage may be a replication of
the primary storage's data but can be queried more effectively.
"""
from ledger.util import F
from plenum.common.txn import NYM, STEWARD, ROLE
from plenum.common.txn import TXN_TYPE, TARGET_NYM
from plenum.common.types import f, Reply


class SecondaryStorage:
    def __init__(self, txnStore, primaryStorage=None):
        self._txnStore = txnStore
        self._primaryStorage = primaryStorage

    def getReply(self, identifier, reqId, **kwargs):
        txn = self._primaryStorage.get(**{f.IDENTIFIER.nm: identifier,
                                           f.REQ_ID.nm: reqId})
        if txn:
            seqNo = txn.get(F.seqNo.name)
            if seqNo:
                txn.update(self._primaryStorage.merkleInfo(seqNo))
        else:
            txn = {}
        return txn

    def getReplies(self, *txnIds, seqNo=None, **kwargs):
        raise NotImplementedError

    def countStewards(self) -> int:
        """Count the number of stewards added to the pool transaction store"""
        allTxns = self._primaryStorage.getAllTxn().values()
        return sum(1 for txn in allTxns if (txn[TXN_TYPE] == NYM) and
                   (txn.get(ROLE) == STEWARD))

    def isSteward(self, nym):
        for txn in self._primaryStorage.getAllTxn().values():
            if txn[TXN_TYPE] == NYM and txn[TARGET_NYM] == nym and \
                            txn.get(ROLE) == STEWARD:
                return True
        return False
