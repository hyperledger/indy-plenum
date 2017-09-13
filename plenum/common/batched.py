from collections import deque
from typing import Any, Iterable

from plenum.common.constants import BATCH, OP_FIELD_NAME
from plenum.common.prepare_batch import split_messages_on_batches
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.crypto.signer import Signer
from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.common.messages.node_messages import Batch
from plenum.common.message_processor import MessageProcessor
from plenum.common.exceptions import InvalidMessageExceedingSizeException
from stp_core.validators.message_length_validator import MessageLenValidator
from stp_core.common.config.util import getConfig

logger = getlogger()


class Batched(MessageProcessor):
    """
    A mixin to allow batching of requests to be send to remotes.
    Assumes a Stack (ZStack or RStack) is mixed
    """

    def __init__(self, config=None):
        """
        :param self: 'NodeStacked'
        :param config: 'stp config'
        """
        self.outBoxes = {}  # type: Dict[int, deque]
        self.stp_config = config or getConfig()
        self.msg_len_val = MessageLenValidator(self.stp_config.MSG_LEN_LIMIT)

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

    def send(self, msg: Any, *
             rids: Iterable[int], signer: Signer = None) -> None:
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
        serializedPayload, err_msg = self.signSerializeAndCheckLen(msg, signer)
        if serializedPayload is None:
            return False, err_msg

        if rids:
            for r in rids:
                self._enqueue(serializedPayload, r, signer)
        else:
            self._enqueueIntoAllRemotes(serializedPayload, signer)
        return True, None

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
                    batches = split_messages_on_batches(list(msgs),
                                                        self._make_batch,
                                                        self._test_batch_len,
                                                        )
                    msgs.clear()
                    if batches:
                        for batch in batches:
                            logger.trace("{} sending payload to {}: {}".format(
                                self, dest, batch))
                            # Setting timeout to never expire
                            self.transmit(
                                batch,
                                rid,
                                timeout=self.messageTimeout,
                                serialized=True)
                    else:
                        logger.warning("Cannot create batch(es) for {}".format(
                            self, dest))
        for rid in removedRemotes:
            logger.warning("{}{} rid {} has been removed"
                           .format(CONNECTION_PREFIX, self, rid),
                           extra={"cli": False})
            msgs = self.outBoxes[rid]
            if msgs:
                self.discard(msgs,
                             "{}rid {} no longer available"
                             .format(CONNECTION_PREFIX, rid),
                             logMethod=logger.debug)
            del self.outBoxes[rid]

    def _make_batch(self, msgs):
        batch = Batch(msgs, None)
        serialized_batch = self.sign_and_serialize(batch)
        return serialized_batch

    def _test_batch_len(self, batch_len):
        return self.msg_len_val.is_len_less_than_limit(batch_len)

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

    def signSerializeAndCheckLen(self, msg, signer=None):
        msg_bytes = self.sign_and_serialize(msg, signer)
        err_msg = None
        try:
            self.msg_len_val.validate(msg_bytes)
        except InvalidMessageExceedingSizeException as ex:
            err_msg = 'Message will be discarded due to {}'.format(ex)
            logger.warning(err_msg)
            msg_bytes = None
        return msg_bytes, err_msg

    def sign_and_serialize(self, msg, signer=None):
        payload = self.prepForSending(msg, signer)
        msg_bytes = self.serializeMsg(payload)
        return msg_bytes
