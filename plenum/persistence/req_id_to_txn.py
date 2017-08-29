from hashlib import sha256
from typing import Optional

from storage.kv_store import KeyValueStorage


class ReqIdrToTxn:
    """
    Stores a map from client identifier, request id tuple to transaction
    sequence number
    """

    def __init__(self, keyValueStorage: KeyValueStorage):
        self._keyValueStorage = keyValueStorage

    @staticmethod
    def getKey(identifier, reqId):
        h = sha256()
        h.update(identifier.encode())
        h.update(str(reqId).encode())
        return h.digest()

    def add(self, identifier, reqId, seqNo):
        key = self.getKey(identifier, reqId)
        self._keyValueStorage.put(key, str(seqNo))

    def addBatch(self, batch):
        self._keyValueStorage.setBatch([(self.getKey(identifier, reqId), str(
            seqNo)) for identifier, reqId, seqNo in batch])

    def get(self, identifier, reqId) -> Optional[int]:
        key = self.getKey(identifier, reqId)
        try:
            val = self._keyValueStorage.get(key)
            return int(val)
        except (KeyError, ValueError):
            return None

    @property
    def size(self):
        return self._keyValueStorage.size

    def close(self):
        self._keyValueStorage.close()
