import sys
from collections import namedtuple
from hashlib import sha256
from typing import NamedTuple, Any, List, Mapping, Optional, TypeVar, Dict

from zeno.common.txn import NOMINATE, PRIMARY, REELECTION, REQDIGEST, REQACK,\
    ORDERED, PROPAGATE, PREPREPARE, REPLY, COMMIT, PREPARE, BATCH, INSTANCE_CHANGE, \
    BLACKLIST

Field = namedtuple("Field", ["nm", "tp"])


class f:  # provides a namespace for reusable field constants
    REQUEST = Field('request', 'Request')
    MSG = Field('msg', str)
    NODE_NAME = Field('nodeName', str)
    NAME = Field("name", str)
    TIE_AMONG = Field("tieAmong", List[str])
    ROUND = Field("round", int)
    CLIENT_ID = Field('clientId', str)
    DIGEST = Field('digest', str)
    PP_SEQ_NO = Field('ppSeqNo', int)  # Pre-Prepare sequence number
    RESULT = Field('result', Any)
    SENDER_NODE = Field('senderNode', str)
    REQ_ID = Field('reqId', int)
    VIEW_NO = Field('viewNo', int)
    INST_ID = Field('instId', int)
    MSGS = Field('messages', List[Mapping])
    SIG = Field('signature', Optional[str])
    SUSP_CODE = Field('suspicionCode', int)
    ELECTION_DATA = Field('electionData', Any)

OP_FIELD_NAME = "op"


class TaggedTupleBase:
    def melted(self):
        """
        Return the tagged tuple in a dictionary form.
        """
        if hasattr(self, "__dict__"):
            m = self.__dict__
        elif hasattr(self, "_asdict"):
            m = self._asdict()
        else:
            raise RuntimeError("Cannot convert argument to a dictionary")
        m[OP_FIELD_NAME] = self.typename
        m.move_to_end(OP_FIELD_NAME, False)
        return m


# noinspection PyProtectedMember
def TaggedTuple(typename, fields):
    cls = NamedTuple(typename, fields)
    if any(field == OP_FIELD_NAME for field in cls._fields):
            raise RuntimeError("field name '{}' is reserved in TaggedTuple".format(OP_FIELD_NAME))
    cls.__bases__ += (TaggedTupleBase,)
    cls.typename = typename
    return cls

Nomination = TaggedTuple(NOMINATE, [
    f.NAME,
    f.INST_ID,
    f.VIEW_NO])

Batch = TaggedTuple(BATCH, [
    f.MSGS,
    f.SIG])

# Reelection messages that nodes send when they find the 2 or more nodes have
# equal nominations for primary. `round` indicates the reelection round
# number. So the first reelection would have round number 1, the one after
# that would have round number 2. If a node receives a reelection message with
# a round number that is not 1 greater than the reelections rounds it has
# already seen then it rejects that message
Reelection = TaggedTuple(REELECTION, [
    f.INST_ID,
    f.ROUND,
    f.TIE_AMONG,
    f.VIEW_NO])

# Declaration of a winner
Primary = TaggedTuple(PRIMARY, [
    f.NAME,
    f.INST_ID,
    f.VIEW_NO])

BlacklistMsg = NamedTuple(BLACKLIST, [
    f.SUSP_CODE,
    f.NODE_NAME])


OPERATION = 'operation'


class Request:
    def __init__(self,
                 clientId: str=None,
                 reqId: int=None,
                 operation: Mapping=None,
                 signature: str=None):
        self.clientId = clientId
        self.reqId = reqId
        self.operation = operation
        self.signature = signature

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __repr__(self):
        return "{}: {}".format(self.__class__.__name__, self.__dict__)

    @property
    def key(self):
        return self.clientId, self.reqId

    @property
    def digest(self):
        return sha256("{}{}".format(*self.key).encode('utf-8')).hexdigest()

    @property
    def reqDigest(self):
        return ReqDigest(self.clientId, self.reqId, self.digest)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)
        return self

    @classmethod
    def fromState(cls, state):
        obj = cls.__new__(cls)
        cls.__setstate__(obj, state)
        return obj


class ReqDigest(NamedTuple(REQDIGEST, [f.CLIENT_ID,
                                       f.REQ_ID,
                                       f.DIGEST])):
    def key(self):
        return self.clientId, self.reqId


RequestAck = TaggedTuple(REQACK, [
    f.REQ_ID])

Ordered = NamedTuple(ORDERED, [
    f.INST_ID,
    f.VIEW_NO,
    f.CLIENT_ID,
    f.REQ_ID,
    f.DIGEST])

# <PROPAGATE, <REQUEST, o, s, c> σc, i>~μi
# s = client sequence number (comes from Aardvark paper)

Propagate = TaggedTuple(PROPAGATE, [
    f.REQUEST])

PrePrepare = TaggedTuple(PREPREPARE, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.CLIENT_ID,
    f.REQ_ID,
    f.DIGEST])

Prepare = TaggedTuple(PREPARE, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.DIGEST])


Commit = TaggedTuple(COMMIT, [
    f.INST_ID,
    f.VIEW_NO,
    f.PP_SEQ_NO,
    f.DIGEST])

Reply = TaggedTuple(REPLY, [
    f.VIEW_NO,
    f.REQ_ID,
    f.RESULT])


InstanceChange = TaggedTuple(INSTANCE_CHANGE, [
    f.VIEW_NO
])

TaggedTuples = None  # type: Dict[str, class]


def loadRegistry():
    global TaggedTuples
    if not TaggedTuples:
        this = sys.modules[__name__]
        TaggedTuples = {getattr(this, x).__name__: getattr(this, x) for x in dir(this)
                        if callable(getattr(getattr(this, x), "melted", None)) and
                        getattr(getattr(this, x), "_fields", None)}

loadRegistry()

ThreePhaseMsg = TypeVar("3PhaseMsg",
                        PrePrepare,
                        Prepare,
                        Commit)
