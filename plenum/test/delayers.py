from typing import Iterable

from plenum.common.types import f, Propagate, PrePrepare, \
    Prepare, Commit, InstanceChange, LedgerStatus, ConsistencyProof, CatchupReq, \
    Nomination, CatchupRep
from plenum.common.constants import OP_FIELD_NAME
from plenum.common.util import getCallableName
from plenum.test.test_client import TestClient


def delayer(seconds, op, senderFilter=None, instFilter: int = None):
    def inner(rx):
        msg, frm = rx
        if msg[OP_FIELD_NAME] == op and \
                (not senderFilter or frm == senderFilter) and \
                (instFilter is None or (f.INST_ID.nm in msg and msg[
                    f.INST_ID.nm] == instFilter)):
            return seconds

    return inner


def delayerMsgTuple(seconds, opType, senderFilter=None, instFilter: int = None):
    """
    Used for nodeInBoxStasher

    :param seconds:
    :param opType:
    :param senderFilter:
    :param instFilter:
    :return:
    """

    def inner(wrappedMsg):
        msg, frm = wrappedMsg
        if isinstance(msg, opType) and \
                (not senderFilter or frm == senderFilter) and \
                (instFilter is None or
                     (f.INST_ID.nm in msg._fields and
                              getattr(msg, f.INST_ID.nm) == instFilter)):
            return seconds

    return inner


def delayerMsgDict(seconds, op, instFilter: int = None):
    def inner(msg):
        if op == msg[OP_FIELD_NAME] and \
                (instFilter is None or (f.INST_ID.nm in msg and msg[
                    f.INST_ID.nm] == instFilter)):
            return seconds

    return inner


def delayerMethod(method, delay):
    def inner(action_pair):
        action, actionId = action_pair
        actionName = getCallableName(action)
        if actionName == method.__name__:
            return delay

    return inner


def nom_delay(delay: float):
    # Delayer of NOMINATE requests
    return delayerMsgTuple(delay, Nomination)


def ppgDelay(delay: float):
    # Delayer of PROPAGATE requests
    return delayerMsgTuple(delay, Propagate)


def ppDelay(delay: float, instId: int=None):
    # Delayer of PRE-PREPARE requests from a particular instance
    return delayerMsgTuple(delay, PrePrepare, instFilter=instId)


def pDelay(delay: float, instId: int=None):
    # Delayer of PREPARE requests from a particular instance
    return delayerMsgTuple(delay, Prepare, instFilter=instId)


def cDelay(delay: float, instId: int=None):
    # Delayer of COMMIT requests from a particular instance
    return delayerMsgTuple(delay, Commit, instFilter=instId)


def icDelay(delay: float):
    # Delayer of INSTANCE-CHANGE requests
    return delayerMsgTuple(delay, InstanceChange)


def lsDelay(delay: float):
    # Delayer of LEDGER_STATUSES requests
    return delayerMsgTuple(delay, LedgerStatus)


def cpDelay(delay: float):
    # Delayer of CONSISTENCY_PROOFS requests
    return delayerMsgTuple(delay, ConsistencyProof)


def cqDelay(delay: float):
    # Delayer of CATCHUP_REQ requests
    return delayerMsgTuple(delay, CatchupReq)


def cr_delay(delay: float):
    # Delayer of CATCHUP_REP requests
    return delayerMsgTuple(delay, CatchupRep)


def delay(what, frm, to, howlong):
    from plenum.test.test_node import TestNode

    if not isinstance(frm, Iterable):
        frm = [frm]
    if not isinstance(to, Iterable):
        to = [to]
    for f in frm:
        for t in to:
            if isinstance(t, TestNode):
                if isinstance(f, TestNode):
                    stasher = t.nodeIbStasher
                elif isinstance(f, TestClient):
                    stasher = t.clientIbStasher
                else:
                    raise TypeError(
                            "from type {} for {} not supported".format(type(f),
                                                                       f))
                stasher.delay(delayerMsgTuple(howlong, what, f.name))
            else:
                raise TypeError(
                        "to type {} for {} not supported".format(type(t), t))


def delayNonPrimaries(nodeSet, instId, delay):
    from plenum.test.test_node import getNonPrimaryReplicas
    nonPrimReps = getNonPrimaryReplicas(nodeSet, instId)
    for r in nonPrimReps:
        r.node.nodeIbStasher.delay(ppDelay(delay, instId))