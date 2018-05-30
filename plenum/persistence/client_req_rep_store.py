from abc import abstractmethod
from typing import Any, Sequence

from common.serializers.serialization import client_req_rep_store_serializer
from plenum.common.request import Request


class ClientReqRepStore:
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @property
    @abstractmethod
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
    def addReject(self, msg: Any, sender: str):
        pass

    @abstractmethod
    def addReply(self, key: str, sender: str,
                 result: Any) -> Sequence[str]:
        pass

    @abstractmethod
    def hasRequest(self, key: str) -> bool:
        pass

    @abstractmethod
    def getRequest(self, key: str) -> Request:
        pass

    @abstractmethod
    def getReplies(self, key: str):
        pass

    @abstractmethod
    def getAcks(self, key: str) -> dict:
        pass

    @abstractmethod
    def getNacks(self, key: str) -> dict:
        pass

    @abstractmethod
    def getRejects(self, key: str) -> dict:
        pass

    def getAllReplies(self, key: str):
        replies = self.getReplies(key)
        errors = self.getNacks(key)
        if not errors:
            errors = {**errors, **self.getRejects(key)}
        return replies, errors

    @property
    @abstractmethod
    def txnFieldOrdering(self):
        raise NotImplementedError

    # noinspection PyAttributeOutsideInit
    @property
    def txnSerializer(self):
        return client_req_rep_store_serializer
