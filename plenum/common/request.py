from hashlib import sha256
from typing import Mapping, NamedTuple, Dict

from common.serializers.serialization import serialize_msg_for_signing
from plenum.common.constants import REQKEY, FORCE, TXN_TYPE, OPERATION_SCHEMA_IS_STRICT
from plenum.common.messages.client_request import ClientMessageValidator
from plenum.common.types import f, OPERATION
from plenum.common.util import getTimeBasedId
from stp_core.types import Identifier
from plenum import PLUGIN_CLIENT_REQUEST_FIELDS


class Request:
    idr_delimiter = ','

    def __init__(self,
                 identifier: Identifier=None,
                 reqId: int=None,
                 operation: Mapping=None,
                 signature: str=None,
                 signatures: Dict[str, str]=None,
                 protocolVersion: int = None,
                 # Intentionally omitting *args
                 **kwargs):
        self._identifier = identifier
        self.signature = signature
        self.signatures = signatures
        self.reqId = reqId
        self.operation = operation
        self.protocolVersion = protocolVersion
        self._digest = None
        for nm in PLUGIN_CLIENT_REQUEST_FIELDS:
            if nm in kwargs:
                setattr(self, nm, kwargs[nm])

    @property
    def digest(self):
        if self._digest is None:
            self._digest = self.getDigest()
        return self._digest

    @property
    def as_dict(self):
        rv = {
            f.REQ_ID.nm: self.reqId,
            OPERATION: self.operation
        }
        if self._identifier is not None:
            rv[f.IDENTIFIER.nm] = self._identifier
        if self.signatures is not None:
            rv[f.SIGS.nm] = self.signatures
        if self.signature is not None:
                rv[f.SIG.nm] = self.signature
        for nm in PLUGIN_CLIENT_REQUEST_FIELDS:
            if hasattr(self, nm):
                rv[nm] = getattr(self, nm)
        if self.protocolVersion is not None:
            rv[f.PROTOCOL_VERSION.nm] = self.protocolVersion
        return rv

    def __eq__(self, other):
        return self.as_dict == other.as_dict

    def __repr__(self):
        return "{}: {}".format(self.__class__.__name__, self.as_dict)

    @property
    def key(self):
        return self.digest

    def getDigest(self):
        return sha256(serialize_msg_for_signing(self.signingState())).hexdigest()

    def __getstate__(self):
        return self.__dict__

    def signingState(self, identifier=None):
        # TODO: separate data, metadata and signature, so that we don't
        # need to have this kind of messages
        dct = {
            f.IDENTIFIER.nm: identifier or self.identifier,
            f.REQ_ID.nm: self.reqId,
            OPERATION: self.operation
        }
        if self.protocolVersion is not None:
            dct[f.PROTOCOL_VERSION.nm] = self.protocolVersion
        return dct

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

    @property
    def txn_type(self):
        return self.operation.get(TXN_TYPE)

    @property
    def identifier(self):
        return self._identifier or self.gen_idr_from_sigs(self.signatures)

    @property
    def all_identifiers(self):
        if self.signatures is None:
            return []
        return sorted(self.signatures.keys())

    @staticmethod
    def gen_req_id():
        return getTimeBasedId()

    @staticmethod
    def gen_idr_from_sigs(signatures: Dict):
        return Request.idr_delimiter.join(sorted(signatures.keys()))

    def add_signature(self, identifier, signature):
        if not isinstance(self.signatures, Dict):
            self.signatures = {}
        self.signatures[identifier] = signature

    def __hash__(self):
        return hash(self.serialized())


class ReqKey(NamedTuple(REQKEY, [f.DIGEST])):
    pass


class SafeRequest(Request, ClientMessageValidator):
    def __init__(self, **kwargs):
        ClientMessageValidator.__init__(self,
                                        operation_schema_is_strict=OPERATION_SCHEMA_IS_STRICT)
        self.validate(kwargs)
        Request.__init__(self, **kwargs)
