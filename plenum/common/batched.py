from collections import deque
from typing import Any, Iterable
from typing import Dict

from plenum.common.constants import BATCH, OP_FIELD_NAME
from stp_core.crypto.signer import Signer
from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.common.messages.node_messages import Batch
from plenum.common.message_processor import MessageProcessor

logger = getlogger()


class Batched(MessageProcessor):
    """
    A mixin to allow batching of requests to be send to remotes.
    Assumes a Stack (ZStack or RStack) is mixed
    """

    def __init__(self):
        """
        :param self: 'NodeStacked'
        """
        self.outBoxes = {}  # type: Dict[int, deque]

    def _enqueue(self, msg: Any, rid: int, signer: Signer) -> None:
        """
        Enqueue the message into the remote's queue.

        :param msg: the message to enqueue
        :param rid: the id of the remote node
        """
        if rid not in self.outBoxes:
            self.outBoxes[rid] = deque()
        self.outBoxes[rid].append(msg)

    def _enqueueIntoAllRemotes(self, msg: Any, signer: Signer) -> None:
        """
        Enqueue the specified message into all the remotes in the nodestack.

        :param msg: the message to enqueue
        """
        for rid in self.remotes.keys():
            self._enqueue(msg, rid, signer)

    def send(self, msg: Any, *rids: Iterable[int], signer: Signer = None) -> None:
        """
        Enqueue the given message into the outBoxes of the specified remotes
         or into the outBoxes of all the remotes if rids is None

        :param msg: the message to enqueue
        :param rids: ids of the remotes to whose outBoxes
         this message must be enqueued
        """
        # Signing (if required) and serializing before enqueueing otherwise
        # each call to `_enqueue` will have to sign it and `transmit` will try
        # to serialize it which is waste of resources
        serializedPayload = self.signAndSerialize(msg, signer)

        if rids:
            for r in rids:
                self._enqueue(serializedPayload, r, signer)
        else:
            self._enqueueIntoAllRemotes(serializedPayload, signer)

    def flushOutBoxes(self) -> None:
        """
        Clear the outBoxes and transmit batched messages to remotes.
        """
        removedRemotes = []
        for rid, msgs in self.outBoxes.items():
            try:
                dest = self.remotes[rid].name
            except KeyError:
                removedRemotes.append(rid)
                continue
            if msgs:
                if len(msgs) == 1:
                    msg = msgs.popleft()
                    # Setting timeout to never expire
                    self.transmit(msg, rid, timeout=self.messageTimeout,
                                  serialized=True)
                    logger.trace(
                        "{} sending msg {} to {}".format(self, msg, dest))
                else:
                    logger.debug(
                        "{} batching {} msgs to {} into one transmission".
                        format(self, len(msgs), dest))
                    logger.trace("    messages: {}".format(msgs))
                    batch = Batch(list(msgs), None)
                    msgs.clear()
                    # don't need to sign the batch, when the composed msgs are
                    # signed
                    payload = self.signAndSerialize(batch)
                    logger.trace("{} sending payload to {}: {}".format(self,
                                                                       dest,
                                                                       payload))
                    # Setting timeout to never expire
                    self.transmit(payload, rid, timeout=self.messageTimeout,
                                  serialized=True)
        for rid in removedRemotes:
            logger.warning("{} rid {} has been removed".format(self, rid),
                           extra={"cli": False})
            msgs = self.outBoxes[rid]
            if msgs:
                self.discard(msgs, "rid {} no longer available".format(rid),
                             logMethod=logger.debug)
            del self.outBoxes[rid]

    def doProcessReceived(self, msg, frm, ident):
        if OP_FIELD_NAME in msg and msg[OP_FIELD_NAME] == BATCH:
            if f.MSGS.nm in msg and isinstance(msg[f.MSGS.nm], list):
                # Removing ping and pong messages from Batch
                relevantMsgs = []
                for m in msg[f.MSGS.nm]:
                    r = self.handlePingPong(m, frm, ident)
                    if not r:
                        relevantMsgs.append(m)

                if not relevantMsgs:
                    return None
                msg[f.MSGS.nm] = relevantMsgs
        return msg

    def signAndSerialize(self, msg, signer=None):
        payload = self.prepForSending(msg, signer)
        return self.serializeMsg(payload)
