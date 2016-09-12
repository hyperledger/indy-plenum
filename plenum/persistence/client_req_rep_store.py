from abc import abstractmethod, abstractproperty
from typing import Any, Sequence

from ledger.serializers.compact_serializer import CompactSerializer
from plenum.common.types import Request


class ClientReqRepStore:
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractproperty
    def lastReqId(self) -> int:
        pass

    @abstractmethod
    def addRequest(self, req: Request):
        pass

    @abstractmethod
    def addAck(self, msg: Any, sender: str):
        pass

    @abstractmethod
    def addNack(self, msg: Any, sender: str):
        pass

    @abstractmethod
    def addReply(self, reqId: int, sender: str, result: Any) -> Sequence[str]:
        pass

    @abstractmethod
    def hasRequest(self, reqId: int) -> bool:
        pass

    @abstractmethod
    def getReplies(self, reqId: int):
        pass

    @abstractmethod
    def getAcks(self, reqId: int) -> dict:
        pass

    @abstractmethod
    def getNacks(self, reqId: int) -> dict:
        pass

    def getAllReplies(self, reqId: int):
        replies = self.getReplies(reqId)
        errors = self.getNacks(reqId)
        return replies, errors

    @abstractproperty
    def txnFieldOrdering(self):
        raise NotImplementedError

    # noinspection PyAttributeOutsideInit
    @property
    def txnSerializer(self):
        # if not self._serializer:
        #     self._serializer = CompactSerializer(fields=self.txnFieldOrdering)
        # return self._serializer
        return CompactSerializer(fields=self.txnFieldOrdering)
