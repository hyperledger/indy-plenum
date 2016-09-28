import json
import os
from collections import OrderedDict
from typing import Any, Sequence, List

from ledger.stores.directory_store import DirectoryStore
from ledger.util import F
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.txn_util import getTxnOrderedFields
from plenum.common.types import Request, f
from plenum.common.util import updateFieldsWithSeqNo
from plenum.persistence.client_req_rep_store import ClientReqRepStore


class ClientReqRepStoreFile(ClientReqRepStore, HasFileStorage):
    def __init__(self, name, baseDir):
        self.baseDir = baseDir
        self.dataDir = "data/clients"
        self.name = name
        HasFileStorage.__init__(self, name=self.name, baseDir=baseDir,
                                dataDir=self.dataDir)
        if not os.path.exists(self.dataLocation):
            os.makedirs(self.dataLocation)
        self.reqStore = DirectoryStore(self.dataLocation, "Requests")
        self._serializer = None

    @property
    def lastReqId(self) -> int:
        reqIds = self.reqStore.keys
        return max(map(int, reqIds)) if reqIds else 0

    def addRequest(self, req: Request):
        reqId = req.reqId
        self.reqStore.put(str(reqId), "0:{}".format(self.serializeReq(req)))

    def addAck(self, msg: Any, sender: str):
        reqId = msg[f.REQ_ID.nm]
        self.reqStore.appendToValue(str(reqId), "A:{}".format(sender))

    def addNack(self, msg: Any, sender: str):
        reqId = msg[f.REQ_ID.nm]
        reason = msg[f.REASON.nm]
        self.reqStore.appendToValue(str(reqId), "N:{}:{}".format(sender,
                                                                 reason))

    def addReply(self, reqId: int, sender: str, result: Any) -> Sequence[str]:
        serializedReply = self.txnSerializer.serialize(result, toBytes=False)
        self.reqStore.appendToValue(str(reqId),
                                    "R:{}:{}".format(sender, serializedReply))
        return len(self._getSerailzedReplies(reqId))

    def hasRequest(self, reqId: int) -> bool:
        return self.reqStore.exists(str(reqId))

    def getReplies(self, reqId: int):
        replies = self._getSerailzedReplies(reqId)
        for sender, reply in replies.items():
            replies[sender] = self.txnSerializer.deserialize(reply)
        return replies

    def getAcks(self, reqId: int) -> List[str]:
        ackLines = self._getLinesWithPrefix(reqId, "A:")
        return [line[2:] for line in ackLines]

    def getNacks(self, reqId: int) -> dict:
        nackLines = self._getLinesWithPrefix(reqId, "N:")
        result = {}
        for line in nackLines:
            sender, reason = line[2:].split(":", 1)
            result[sender] = reason
        return result

    @property
    def txnFieldOrdering(self):
        fields = getTxnOrderedFields()
        return updateFieldsWithSeqNo(fields)

    @staticmethod
    def serializeReq(req: Request):
        return json.dumps(req.__getstate__())

    def _getLinesWithPrefix(self, reqId: int, prefix: str):
        data = self.reqStore.get(str(reqId))
        return [line for line in data.split(os.linesep)
                if line.startswith(prefix)]

    def _getSerailzedReplies(self, reqId: int):
        replyLines = self._getLinesWithPrefix(reqId, "R:")
        result = {}
        for line in replyLines:
            sender, reply = line[2:].split(":", 1)
            result[sender] = reply
        return result

