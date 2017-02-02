from hashlib import sha256

import leveldb as leveldb
from typing import Optional


class ReqIdToTxn:
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


class ReqIdToTxnLevelDB(ReqIdToTxn):
    def __init__(self, dbPath):
        self.db = leveldb.LevelDB(dbPath)

    def getKey(self, identifier, reqId):
        h = sha256()
        h.update(identifier.encode())
        h.update(str(reqId).encode())
        return h.digest()

    def add(self, identifier, reqId, seqNo):
        key = self.getKey(identifier, reqId)
        self.db.Put(key, str(seqNo).encode())

    def get(self, identifier, reqId) -> Optional[int]:
        key = self.getKey(identifier, reqId)
        val = self.db.Get(key)
        try:
            return int(val)
        except:
            return None
