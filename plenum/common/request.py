from hashlib import sha256
from typing import Mapping, NamedTuple

from plenum.common.signing import serializeMsg
from plenum.common.txn import REQDIGEST
from plenum.common.types import Identifier, f


class Request:
    def __init__(self,
                 identifier: Identifier=None,
                 reqId: int=None,
                 operation: Mapping=None,
                 signature: str=None):
        self.identifier = identifier
        self.reqId = reqId
        self.operation = operation
        self.signature = signature

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "{}: {}".format(self.__class__.__name__, self.__dict__)

    @property
    def key(self):
        return self.identifier, self.reqId

    @property
    def digest(self):
        # The digest needs to be of the whole request. If only client id and
        # request id are used to construct digest, then a malicious client might
        # send different operations to different nodes and the nodes will not
        # realize an have different ledgers.
        return sha256(serializeMsg(self.__dict__)).hexdigest()
        # DEPR
        # return sha256("{}{}".format(*self.key).encode('utf-8')).hexdigest()

    @property
    def reqDigest(self):
        return ReqDigest(self.identifier, self.reqId, self.digest)

    def __getstate__(self):
        return self.__dict__

    def getSigningState(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)
        return self

    @classmethod
    def fromState(cls, state):
        obj = cls.__new__(cls)
        cls.__setstate__(obj, state)
        return obj


class ReqDigest(NamedTuple(REQDIGEST, [f.IDENTIFIER,
                                       f.REQ_ID,
                                       f.DIGEST])):
    def key(self):
        return self.identifier, self.reqId
