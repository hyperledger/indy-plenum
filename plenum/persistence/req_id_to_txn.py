from hashlib import sha256
from typing import Optional

from plenum.persistence.kv_store_leveldb import KVStoreLeveldb


class ReqIdrToTxn:
    """
    Stores a map from client identifier, request id tuple to transaction
    sequence number
    """
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def add(self, identifier, reqId, seqNo):
        raise NotImplementedError

    def get(self, identifier, reqId):
        raise NotImplementedError


class ReqIdrToTxnKVStore(ReqIdrToTxn):
    def __init__(self, dbPath):
        self.dbPath = dbPath
        self.db = KVStoreLeveldb(dbPath)

    def getKey(self, identifier, reqId):
        h = sha256()
        h.update(identifier.encode())
        h.update(str(reqId).encode())
        return h.digest()

    def add(self, identifier, reqId, seqNo):
        key = self.getKey(identifier, reqId)
        self.db.set(key, str(seqNo))

    def addBatch(self, batch):
        self.db.setBatch([(self.getKey(identifier, reqId), str(seqNo))
                          for identifier, reqId, seqNo in batch])

    def get(self, identifier, reqId) -> Optional[int]:
        key = self.getKey(identifier, reqId)
        try:
            val = self.db.get(key)
            return int(val)
        except (KeyError, ValueError):
            return None

    def close(self):
        self.db.close()
