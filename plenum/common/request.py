from hashlib import sha256
from typing import Mapping, NamedTuple

from common.serializers.serialization import serialize_msg_for_signing
from plenum.common.constants import REQDIGEST, REQKEY, FORCE
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.types import f, OPERATION
from stp_core.types import Identifier


class Request:
    def __init__(self,
                 identifier: Identifier=None,
                 reqId: int=None,
                 operation: Mapping=None,
                 signature: str=None):
        self.identifier = identifier
        self.reqId = reqId
        self.operation = operation
        self.digest = self.getDigest()
        self.signature = signature

    @property
    def as_dict(self):
        return {
            f.IDENTIFIER.nm: self.identifier,
            f.REQ_ID.nm: self.reqId,
            OPERATION: self.operation,
            f.SIG.nm: self.signature
        }

    def __eq__(self, other):
        return self.as_dict == other.as_dict

    def __repr__(self):
        return "{}: {}".format(self.__class__.__name__, self.as_dict)

    @property
    def key(self):
        return self.identifier, self.reqId

    def getDigest(self):
        return sha256(serialize_msg_for_signing(self.signingState)).hexdigest()

    @property
    def reqDigest(self):
        return ReqDigest(self.identifier, self.reqId, self.digest)

    def __getstate__(self):
        return self.__dict__

    @property
    def signingState(self):
        return {
            f.IDENTIFIER.nm: self.identifier,
            f.REQ_ID.nm: self.reqId,
            OPERATION: self.operation
        }

    def __setstate__(self, state):
        self.__dict__.update(state)
        return self

    @classmethod
    def fromState(cls, state):
        obj = cls.__new__(cls)
        cls.__setstate__(obj, state)
        return obj

    def serialized(self):
        return serialize_msg_for_signing(self.__getstate__())

    def isForced(self):
        force = self.operation.get(FORCE)
        return str(force) == 'True'

    def __hash__(self):
        return hash(self.serialized())


class ReqDigest(NamedTuple(REQDIGEST, [f.IDENTIFIER,
                                       f.REQ_ID,
                                       f.DIGEST])):
    @property
    def key(self):
        return self.identifier, self.reqId


class ReqKey(NamedTuple(REQKEY, [f.IDENTIFIER, f.REQ_ID])):
    pass


class SafeRequest(Request, ClientMessageValidator):

    def __init__(self, **kwargs):
        self.validate(kwargs)
        super().__init__(**kwargs)
