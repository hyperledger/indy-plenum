from hashlib import sha256

import leveldb as leveldb
from typing import Optional

from plenum.persistence.util import removeLockFiles


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


class ReqIdrToTxnLevelDB(ReqIdrToTxn):
    def __init__(self, dbPath):
        self.dbPath = dbPath
        self.db = leveldb.LevelDB(dbPath)

    def getKey(self, identifier, reqId):
        h = sha256()
        h.update(identifier.encode())
        h.update(str(reqId).encode())
        return h.digest()

    def add(self, identifier, reqId, seqNo):
        key = self.getKey(identifier, reqId)
        self.db.Put(key, str(seqNo).encode())

    def addBatch(self, batch):
        b = leveldb.WriteBatch()
        for identifier, reqId, seqNo in batch:
            key = self.getKey(identifier, reqId)
            b.Put(key, str(seqNo).encode())
        self.db.Write(b, sync=False)

    def get(self, identifier, reqId) -> Optional[int]:
        key = self.getKey(identifier, reqId)
        try:
            val = self.db.Get(key)
            return int(val)
        except (KeyError, ValueError):
            return None

    def close(self):
        removeLockFiles(self.dbPath)
        del self.db
        self.db = None
