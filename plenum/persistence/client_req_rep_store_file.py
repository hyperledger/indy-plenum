import os
from collections import namedtuple
from typing import Any, List, Dict

from plenum.common.constants import REQACK, REQNACK, REPLY, REJECT
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.request import Request
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.types import f
from plenum.common.util import updateFieldsWithSeqNo
from plenum.persistence.client_req_rep_store import ClientReqRepStore
from storage.directory_store import DirectoryStore


class ClientReqRepStoreFile(ClientReqRepStore, HasFileStorage):
    LinePrefixes = namedtuple(
        'LP', ['Request', REQACK, REQNACK, REJECT, REPLY])

    def __init__(self, dataLocation):
        assert dataLocation is not None
        HasFileStorage.__init__(self, dataLocation)
        if not os.path.exists(self.dataLocation):
            os.makedirs(self.dataLocation)
        self.reqStore = DirectoryStore(self.dataLocation, "Requests")
        self._serializer = None
        self.linePrefixes = self.LinePrefixes('0', 'A', 'N', 'J', 'R')
        self.delimiter = '~'

    @property
    def lastReqId(self) -> int:
        reqIds = [self.items_from_key(key)[1] for key in self.reqStore.keys]
        return max(map(int, reqIds)) if reqIds else 0

    @staticmethod
    def items_from_key(key):
        return key.split(',')

    def addRequest(self, req: Request):
        key = req.digest
        self.reqStore.appendToValue(key, "{}{}{}".
                                    format(self.linePrefixes.Request,
                                           self.delimiter,
                                           self.serializeReq(req)))

    def addAck(self, msg: Any, sender: str):
        key = msg[f.DIGEST.nm]
        self.reqStore.appendToValue(key, "{}{}{}".
                                    format(self.linePrefixes.REQACK,
                                           self.delimiter, sender))

    def addNack(self, msg: Any, sender: str):
        key = msg[f.DIGEST.nm]
        reason = msg[f.REASON.nm]
        self.reqStore.appendToValue(key, "{}{}{}{}{}".
                                    format(self.linePrefixes.REQNACK,
                                           self.delimiter, sender,
                                           self.delimiter, reason))

    def addReject(self, msg: Any, sender: str):
        key = msg[f.DIGEST.nm]
        reason = msg[f.REASON.nm]
        self.reqStore.appendToValue(key, "{}{}{}{}{}".
                                    format(self.linePrefixes.REJECT,
                                           self.delimiter, sender,
                                           self.delimiter, reason))

    def addReply(self, key: str, sender: str,
                 result: Any) -> int:
        serializedReply = self.txnSerializer.serialize(result, toBytes=False)
        self.reqStore.appendToValue(key,
                                    "{}{}{}{}{}".
                                    format(self.linePrefixes.REPLY,
                                           self.delimiter, sender,
                                           self.delimiter, serializedReply))
        return len(self._getSerializedReplies(key))

    def hasRequest(self, key: str) -> bool:
        return self.reqStore.exists(key)

    def getRequest(self, key: str) -> Request:
        for r in self._getLinesWithPrefix(
                key, "{}{}".format(
                    self.linePrefixes.Request, self.delimiter)):
            return self.deserializeReq(r[2:])

    def getReplies(self, key: str):
        replies = self._getSerializedReplies(key)
        for sender, reply in replies.items():
            replies[sender] = self.txnSerializer.deserialize(reply)
        return replies

    def getAcks(self, key: str) -> List[str]:
        ackLines = self._getLinesWithPrefix(key, "{}{}".
                                            format(self.linePrefixes.REQACK,
                                                   self.delimiter))
        return [line[2:] for line in ackLines]

    def getNacks(self, key: str) -> dict:
        nackLines = self._getLinesWithPrefix(key, "{}{}".
                                             format(self.linePrefixes.REQNACK,
                                                    self.delimiter))
        result = {}
        for line in nackLines:
            sender, reason = line[2:].split(self.delimiter, 1)
            result[sender] = reason
        return result

    def getRejects(self, key: str) -> dict:
        nackLines = self._getLinesWithPrefix(
            key, "{}{}".format(
                self.linePrefixes.REJECT, self.delimiter))
        result = {}
        for line in nackLines:
            sender, reason = line[2:].split(self.delimiter, 1)
            result[sender] = reason
        return result

    @property
    def txnFieldOrdering(self):
        fields = getTxnOrderedFields()
        return updateFieldsWithSeqNo(fields)

    def serializeReq(self, req: Request) -> str:
        return self.txnSerializer.serialize(req.__getstate__(), toBytes=False)

    def deserializeReq(self, serReq: str) -> Request:
        return Request.fromState(
            self.txnSerializer.deserialize(serReq))

    def _getLinesWithPrefix(self, key: str,
                            prefix: str) -> List[str]:
        data = self.reqStore.get(key)
        return [line for line in data.splitlines()
                if line.startswith(prefix)] if data else []

    def _getSerializedReplies(self, key: str) -> \
            Dict[str, str]:
        replyLines = self._getLinesWithPrefix(key, "{}{}".
                                              format(self.linePrefixes.REPLY,
                                                     self.delimiter))
        result = {}
        for line in replyLines:
            sender, reply = line[2:].split(self.delimiter, 1)
            result[sender] = reply
        return result
