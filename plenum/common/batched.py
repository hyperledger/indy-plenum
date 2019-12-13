from collections import deque
from typing import Any, Iterable, Dict

from plenum.common.util import z85_to_friendly
from plenum.common.constants import BATCH, OP_FIELD_NAME
from plenum.common.metrics_collector import NullMetricsCollector, MetricsName
from plenum.common.prepare_batch import split_messages_on_batches
from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.crypto.signer import Signer
from stp_core.common.log import getlogger
from plenum.common.types import f
from plenum.common.messages.node_messages import Batch
from plenum.common.message_processor import MessageProcessor
from stp_core.validators.message_length_validator import MessageLenValidator
from stp_core.common.config.util import getConfig

logger = getlogger()


class Batched(MessageProcessor):
    """
    A mixin to allow batching of requests to be send to remotes.
    Assumes a Stack (ZStack or RStack) is mixed
    """

    def __init__(self, config=None, metrics=NullMetricsCollector()):
        """
        :param self: 'NodeStacked'
        :param config: 'stp config'
        """
        self.outBoxes = {}  # type: Dict[int, deque]
        self.stp_config = config or getConfig()
        self.msg_len_val = MessageLenValidator(self.stp_config.MSG_LEN_LIMIT)
        self.metrics = metrics
        self.enabled = self.stp_config.TRANSPORT_BATCH_ENABLED

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

    def send(self,
             msg: Any, *
             rids: Iterable[int],
             signer: Signer = None,
             message_splitter=None) -> None:
        """
        Enqueue the given message into the outBoxes of the specified remotes
        or into the outBoxes of all the remotes if rids is None

        :param msg: the message to enqueue
        :param rids: ids of the remotes to whose outBoxes
            this message must be enqueued
        :param message_splitter: callable that splits msg on
            two smaller messages
        """
        # Signing (if required) and serializing before enqueueing otherwise
        # each call to `_enqueue` will have to sign it and `transmit` will try
        # to serialize it which is waste of resources
        message_parts, err_msg = \
            self.prepare_for_sending(msg, signer, message_splitter)

        # TODO: returning breaks contract of super class
        if err_msg is not None:
            return False, err_msg

        if rids:
            for r in rids:
                for part in message_parts:
                    self._enqueue(part, r, signer)
        else:
            for part in message_parts:
                self._enqueueIntoAllRemotes(part, signer)
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
                if self._should_batch(msgs):
                    logger.trace(
                        "{} batching {} msgs to {} into fewer transmissions".
                        format(self, len(msgs), dest))
                    logger.trace("    messages: {}".format(msgs))
                    batches = split_messages_on_batches(list(msgs),
                                                        self._make_batch,
                                                        self._test_batch_len,
                                                        )
                    msgs.clear()
                    if batches:
                        for batch, size in batches:
                            logger.trace("{} sending payload to {}: {}".format(
                                self, dest, batch))
                            self.metrics.add_event(MetricsName.TRANSPORT_BATCH_SIZE, size)
                            # Setting timeout to never expire
                            self.transmit(
                                batch,
                                rid,
                                timeout=self.messageTimeout,
                                serialized=True,
                                is_batch=True
                            )
                    else:
                        logger.error("{} cannot create batch(es) for {}".format(self, dest))
                else:
                    while msgs:
                        msg = msgs.popleft()
                        logger.trace(
                            "{} sending msg {} to {}".format(self, msg, dest))
                        self.metrics.add_event(MetricsName.TRANSPORT_BATCH_SIZE, 1)
                        # Setting timeout to never expire
                        self.transmit(msg, rid, timeout=self.messageTimeout,
                                      serialized=True, is_batch=False)

        for rid in removedRemotes:
            logger.info("{}{} has removed rid {}".
                        format(CONNECTION_PREFIX, self, z85_to_friendly(rid)), extra={"cli": False})
            msgs = self.outBoxes[rid]
            if msgs:
                self.discard(msgs,
                             "{}rid {} no longer available"
                             .format(CONNECTION_PREFIX,
                                     z85_to_friendly(rid)),
                             logMethod=logger.debug)
            del self.outBoxes[rid]

    def _make_batch(self, msgs):
        if len(msgs) > 1:
            batch = Batch(msgs, None)
            serialized_batch = self.sign_and_serialize(batch)
        else:
            serialized_batch = msgs[0]
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

    def prepare_for_sending(self, msg, signer,
                            message_splitter=lambda x: None):
        large_msg_parts = [msg]
        fine_msg_parts = []
        while len(large_msg_parts):
            part = large_msg_parts.pop()
            part_bytes = self.sign_and_serialize(part, signer)
            if self.msg_len_val.is_len_less_than_limit(len(part_bytes)):
                fine_msg_parts.append(part_bytes)
                continue
            if message_splitter is not None:
                smaller_parts = message_splitter(part)
                if smaller_parts is not None:
                    large_msg_parts.extend(smaller_parts)
                    continue

            large_msg_parts.append(part)
            break

        if len(large_msg_parts):
            return None, "message is too large and cannot be split"

        return fine_msg_parts, None

    def sign_and_serialize(self, msg, signer=None):
        payload = self.prepForSending(msg, signer)
        msg_bytes = self.serializeMsg(payload)
        return msg_bytes

    def _should_batch(self, msgs):
        return self.enabled and len(msgs) > 1
