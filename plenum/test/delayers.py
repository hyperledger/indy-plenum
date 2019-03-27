import random
from typing import Iterable, List

from plenum.common.request import Request

from plenum.common.messages.node_messages import Nomination, Reelection, Primary, \
    Propagate, PrePrepare, Prepare, Commit, Checkpoint, InstanceChange, LedgerStatus, \
    ConsistencyProof, CatchupReq, CatchupRep, ViewChangeDone, MessageReq, MessageRep, CurrentState
from plenum.common.constants import OP_FIELD_NAME, MESSAGE_REQUEST, MESSAGE_RESPONSE
from plenum.common.types import f
from plenum.common.util import getCallableName

DEFAULT_DELAY = 600


def delayer(seconds, op, senderFilter=None, instFilter: int = None):
    def inner(rx):
        msg, frm = rx
        if msg[OP_FIELD_NAME] == op and \
                (not senderFilter or frm == senderFilter) and \
                (instFilter is None or (f.INST_ID.nm in msg and msg[
                    f.INST_ID.nm] == instFilter)):
            return seconds

    return inner


def delayerMsgTuple(seconds, opType, senderFilter=None,
                    instFilter: int = None,
                    ledgerFilter: int = None):
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
                  getattr(msg, f.INST_ID.nm) == instFilter)) and \
                (ledgerFilter is None or
                 f.LEDGER_ID.nm in msg._fields and
                 getattr(msg, f.LEDGER_ID.nm) == ledgerFilter):
            return seconds

    if hasattr(opType, 'typename'):
        inner.__name__ = opType.typename
    else:
        inner.__name__ = opType.__name__
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


def nom_delay(delay: float = DEFAULT_DELAY, inst_id=None, sender_filter: str = None):
    # Delayer of NOMINATE requests
    return delayerMsgTuple(
        delay, Nomination, instFilter=inst_id, senderFilter=sender_filter)


def prim_delay(delay: float = DEFAULT_DELAY, inst_id=None, sender_filter: str = None):
    # Delayer of PRIMARY requests
    return delayerMsgTuple(
        delay, Primary, instFilter=inst_id, senderFilter=sender_filter)


def rel_delay(delay: float = DEFAULT_DELAY, inst_id=None, sender_filter: str = None):
    # Delayer of REELECTION requests
    return delayerMsgTuple(
        delay, Reelection, instFilter=inst_id, senderFilter=sender_filter)


def ppgDelay(delay: float = DEFAULT_DELAY, sender_filter: str = None):
    # Delayer of PROPAGATE requests
    return delayerMsgTuple(delay, Propagate, senderFilter=sender_filter)


def ppDelay(delay: float = DEFAULT_DELAY, instId: int = None, sender_filter: str = None):
    # Delayer of PRE-PREPARE requests from a particular instance
    return delayerMsgTuple(delay, PrePrepare, instFilter=instId,
                           senderFilter=sender_filter)


def pDelay(delay: float = DEFAULT_DELAY, instId: int = None, sender_filter: str = None):
    # Delayer of PREPARE requests from a particular instance
    return delayerMsgTuple(
        delay, Prepare, instFilter=instId, senderFilter=sender_filter)


def cDelay(delay: float = DEFAULT_DELAY, instId: int = None, sender_filter: str = None):
    # Delayer of COMMIT requests from a particular instance
    return delayerMsgTuple(
        delay, Commit, instFilter=instId, senderFilter=sender_filter)


def icDelay(delay: float = DEFAULT_DELAY):
    # Delayer of INSTANCE-CHANGE requests
    return delayerMsgTuple(delay, InstanceChange)


def vcd_delay(delay: float = DEFAULT_DELAY):
    # Delayer of VIEW_CHANGE_DONE requests
    return delayerMsgTuple(delay, ViewChangeDone)


def cs_delay(delay: float = DEFAULT_DELAY):
    # Delayer of CURRENT_STATE requests
    return delayerMsgTuple(delay, CurrentState)


def chk_delay(delay: float = DEFAULT_DELAY, instId: int = None, sender_filter: str = None):
    # Delayer of CHECKPOINT requests
    return delayerMsgTuple(delay, Checkpoint, instFilter=instId, senderFilter=sender_filter)


def lsDelay(delay: float = DEFAULT_DELAY, ledger_filter=None):
    # Delayer of LEDGER_STATUSES requests
    return delayerMsgTuple(delay, LedgerStatus, ledgerFilter=ledger_filter)


def cpDelay(delay: float = DEFAULT_DELAY):
    # Delayer of CONSISTENCY_PROOFS requests
    return delayerMsgTuple(delay, ConsistencyProof)


def cqDelay(delay: float = DEFAULT_DELAY):
    # Delayer of CATCHUP_REQ requests
    return delayerMsgTuple(delay, CatchupReq)


def cr_delay(delay: float = DEFAULT_DELAY, ledger_filter=None):
    # Delayer of CATCHUP_REP requests
    return delayerMsgTuple(delay, CatchupRep, ledgerFilter=ledger_filter)


def req_delay(delay: float = DEFAULT_DELAY):
    # Delayer of Request requests
    return delayerMsgTuple(delay, Request)


def msg_req_delay(delay: float = DEFAULT_DELAY, types_to_delay: List = None):
    # Delayer of MessageReq messages
    def specific_msgs(msg):
        if isinstance(
                msg[0], MessageReq) and (
                not types_to_delay or msg[0].msg_type in types_to_delay):
            return delay

    specific_msgs.__name__ = MESSAGE_REQUEST
    return specific_msgs


def msg_rep_delay(delay: float = DEFAULT_DELAY, types_to_delay: List = None):
    # Delayer of MessageRep messages
    def specific_msgs(msg):
        if isinstance(
                msg[0], MessageRep) and (
                not types_to_delay or msg[0].msg_type in types_to_delay):
            return delay

    specific_msgs.__name__ = MESSAGE_RESPONSE
    return specific_msgs


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
                else:
                    raise TypeError(
                        "from type {} for {} not supported".format(type(f),
                                                                   f))
                stasher.delay(delayerMsgTuple(howlong, what, f.name))
            else:
                raise TypeError(
                    "to type {} for {} not supported".format(type(t), t))


def delayNonPrimaries(txnPoolNodeSet, instId, delay):
    from plenum.test.test_node import getNonPrimaryReplicas
    nonPrimReps = getNonPrimaryReplicas(txnPoolNodeSet, instId)
    for r in nonPrimReps:
        r.node.nodeIbStasher.delay(ppDelay(delay, instId))
    return nonPrimReps


def delay_messages(typ, nodes, inst_id, delay=None,
                   min_delay=None, max_delay=None):
    if typ == 'election':
        delay_meths = (nom_delay, prim_delay, rel_delay)
    elif typ == '3pc':
        delay_meths = (ppDelay, pDelay, cDelay)
    else:
        RuntimeError('Unknown type')
    assert delay is not None or (
            min_delay is not None and max_delay is not None)
    for node in nodes:
        if delay:
            d = delay
        else:
            d = min_delay + random.randint(0, max_delay - min_delay)
        for meth in delay_meths:
            node.nodeIbStasher.delay(meth(d, inst_id))
            for other_node in [n for n in nodes if n != node]:
                other_node.nodeIbStasher.delay(meth(d, inst_id, node.name))


def delay_election_messages(nodes, inst_id, delay=None, min_delay=None,
                            max_delay=None):
    # Delay election message
    delay_messages('election', nodes, inst_id, delay, min_delay, max_delay)


def delay_3pc_messages(nodes, inst_id, delay=None, min_delay=None,
                       max_delay=None):
    # Delay 3 phase commit message
    delay_messages('3pc', nodes, inst_id, delay, min_delay, max_delay)


def reset_delays_and_process_delayeds(nodes):
    for node in nodes:
        node.reset_delays_and_process_delayeds()


def reset_delays_and_process_delayeds_for_client(nodes):
    for node in nodes:
        node.reset_delays_and_process_delayeds_for_clients()

