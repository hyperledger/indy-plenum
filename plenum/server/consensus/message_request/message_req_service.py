import logging

from plenum.common.constants import PREPREPARE, PREPARE, COMMIT, VIEW_CHANGE, NEW_VIEW
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.exceptions import IncorrectMessageForHandlingException
from plenum.common.messages.internal_messages import MissingMessage, CheckpointStabilized, ViewChangeStarted
from plenum.common.messages.node_messages import MessageReq, MessageRep, Ordered
from plenum.common.metrics_collector import measure_time, MetricsName, NullMetricsCollector
from plenum.common.router import Subscription
from plenum.common.types import f
from plenum.common.util import compare_3PC_keys
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.server.consensus.message_request.message_handlers import PreprepareHandler, PrepareHandler, CommitHandler, \
    ViewChangeHandler, NewViewHandler
from plenum.server.consensus.utils import replica_name_to_node_name
from stp_core.common.log import getlogger


class MessageReqService:
    def __init__(self,
                 data: ConsensusSharedData,
                 bus: InternalBus,
                 network: ExternalBus,
                 metrics=NullMetricsCollector()):
        self._logger = getlogger()
        self._data = data
        self._bus = bus
        self._subscription = Subscription()
        self._subscription.subscribe(bus, MissingMessage, self.process_missing_message)
        self._subscription.subscribe(bus, Ordered, self.process_ordered)
        self._subscription.subscribe(bus, ViewChangeStarted, self.process_view_change_started)
        self._subscription.subscribe(bus, CheckpointStabilized, self.process_checkpoint_stabilized)

        self._network = network
        self._subscription.subscribe(network, MessageReq, self.process_message_req)
        self._subscription.subscribe(network, MessageRep, self.process_message_rep)

        self.metrics = metrics
        self.handlers = {
            PREPREPARE: PreprepareHandler(self._data),
            PREPARE: PrepareHandler(self._data),
            COMMIT: CommitHandler(self._data),
            VIEW_CHANGE: ViewChangeHandler(self._data),
            NEW_VIEW: NewViewHandler(self._data)
        }
        self.three_pc_handlers = {PREPREPARE, PREPARE, COMMIT}

    @measure_time(MetricsName.PROCESS_MESSAGE_REQ_TIME)
    def process_message_req(self, msg: MessageReq, frm):
        # Assumes a shared memory architecture. In case of multiprocessing,
        # RPC architecture, use deques to communicate the message and node will
        # maintain a unique internal message id to correlate responses.
        msg_type = msg.msg_type
        if msg_type not in self.handlers.keys():
            self.discard(msg, "Unknown message type {}".format(msg_type), self._logger.warning)
            return
        handler = self.handlers[msg_type]
        try:
            resp = handler.process_message_req(msg)
        except IncorrectMessageForHandlingException as e:
            self.discard(e.msg, e.reason, e.log_method)
            return

        if not resp:
            return

        with self.metrics.measure_time(MetricsName.SEND_MESSAGE_REP_TIME):
            self._network.send(MessageRep(**{
                f.MSG_TYPE.nm: msg_type,
                f.PARAMS.nm: msg.params,
                f.MSG.nm: resp
            }), dst=[replica_name_to_node_name(frm), ])

    @measure_time(MetricsName.PROCESS_MESSAGE_REP_TIME)
    def process_message_rep(self, msg: MessageRep, frm):
        msg_type = msg.msg_type
        if msg_type not in self.handlers.keys():
            self.discard(msg, "Unknown message type {}".format(msg_type), self._logger.warning)
            return
        if msg.msg is None:
            self._logger.debug('{} got null response for requested {} from {}'.
                               format(self, msg_type, frm))
            return
        handler = self.handlers[msg_type]
        try:
            validated_msg, frm = handler.extract_message(msg, frm)
            self._network.process_incoming(validated_msg, frm)
        except IncorrectMessageForHandlingException as e:
            self.discard(e.msg, e.reason, e.log_method)

    @measure_time(MetricsName.SEND_MESSAGE_REQ_TIME)
    def process_missing_message(self, msg: MissingMessage):
        if msg.inst_id != self._data.inst_id:
            return
        # TODO: Using a timer to retry would be a better thing to do
        self._logger.trace('{} requesting {} for {} from {}'.format(
            self, msg.msg_type, msg.key, msg.dst))
        handler = self.handlers[msg.msg_type]

        try:
            params = handler.prepare_msg_to_request(msg.key, msg.stash_data)
        except IncorrectMessageForHandlingException as e:
            self.discard(e.msg, e.reason, e.log_method)
            return
        if params:
            self._network.send(MessageReq(**{
                f.MSG_TYPE.nm: msg.msg_type,
                f.PARAMS.nm: params
            }), dst=msg.dst)

    def process_ordered(self, msg: Ordered):
        for msg_type, handler in self.handlers.items():
            if msg_type not in self.three_pc_handlers:
                continue
            handler.requested_messages.pop((msg.viewNo, msg.ppSeqNo), None)

    def process_view_change_started(self, msg: ViewChangeStarted):
        for msg_type, handler in self.handlers.items():
            handler.cleanup()

    def process_checkpoint_stabilized(self, msg: CheckpointStabilized):
        for msg_type, handler in self.handlers.items():
            if msg_type not in self.three_pc_handlers:
                continue
            for key in list(handler.requested_messages.keys()):
                if compare_3PC_keys(key, msg.last_stable_3pc) >= 0:
                    handler.requested_messages.pop(key)

    def discard(self, msg, reason, logMethod=logging.error, cliOutput=False):
        """
        Discard a message and log a reason using the specified `logMethod`.

        :param msg: the message to discard
        :param reason: the reason why this message is being discarded
        :param logMethod: the logging function to be used
        :param cliOutput: if truthy, informs a CLI that the logged msg should
        be printed
        """
        reason = "" if not reason else " because {}".format(reason)
        logMethod("{} discarding message {}{}".format(self, msg, reason),
                  extra={"cli": cliOutput})

    @property
    def name(self):
        return self._data.name

    def __str__(self) -> str:
        return "{} - MessageReq3pcService".format(self._data.name)
