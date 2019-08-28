from typing import Dict, List

from plenum.common.constants import LEDGER_STATUS, PREPREPARE, CONSISTENCY_PROOF, \
    PROPAGATE, PREPARE, COMMIT
from plenum.common.event_bus import InternalBus, ExternalBus
from plenum.common.messages.internal_messages import Missing3pcMessage
from plenum.common.messages.node_messages import MessageReq, MessageRep
from plenum.common.metrics_collector import measure_time, MetricsName, NullMetricsCollector
from plenum.common.types import f
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from stp_core.common.log import getlogger
from plenum.server.message_handlers import LedgerStatusHandler, \
    ConsistencyProofHandler, PreprepareHandler, PrepareHandler, PropagateHandler, \
    CommitHandler


class MessageReq3pcService:
    def __init__(self,
                 data: ConsensusSharedData,
                 bus: InternalBus,
                 network: ExternalBus,
                 metrics=NullMetricsCollector()):
        self._logger = getlogger()
        self._data = data
        self._bus = bus
        self._bus.subscribe(Missing3pcMessage, self.process_missing_message)

        self._network = network
        self._network.subscribe(MessageReq, self.process_message_req)
        self._network.subscribe(MessageRep, self.process_message_rep)

        self.metrics = metrics
        self.handlers = {
            PREPREPARE: PreprepareHandler(self._data),
            PREPARE: PrepareHandler(self._data),
            COMMIT: CommitHandler(self._data),
        }

    @measure_time(MetricsName.PROCESS_MESSAGE_REQ_TIME)
    def process_message_req(self, msg: MessageReq, frm):
        # Assumes a shared memory architecture. In case of multiprocessing,
        # RPC architecture, use deques to communicate the message and node will
        # maintain a unique internal message id to correlate responses.
        msg_type = msg.msg_type
        handler = self.handlers[msg_type]
        resp = handler.serve(msg)

        if not resp:
            return

        with self.metrics.measure_time(MetricsName.SEND_MESSAGE_REP_TIME):
            self._network.send(MessageRep(**{
                f.MSG_TYPE.nm: msg_type,
                f.PARAMS.nm: msg.params,
                f.MSG.nm: resp
            }), dst=[frm, ])

    @measure_time(MetricsName.PROCESS_MESSAGE_REP_TIME)
    def process_message_rep(self, msg: MessageRep, frm):
        msg_type = msg.msg_type
        if msg.msg is None:
            self._logger.debug('{} got null response for requested {} from {}'.
                               format(self, msg_type, frm))
            return
        handler = self.handlers[msg_type]
        validated_msg = handler.process(msg, frm)
        self._network.process_incoming(validated_msg, frm)

    @measure_time(MetricsName.SEND_MESSAGE_REQ_TIME)
    def process_missing_message(self, msg: Missing3pcMessage):
        if msg.inst_id != self._data.inst_id:
            return
        # TODO: Using a timer to retry would be a better thing to do
        self._logger.trace('{} requesting {} for {} from {}'.format(
            self, msg.msg_type, msg.three_pc_key, msg.dst))
        handler = self.handlers[msg.msg_type]

        params = handler.prepare_msg_to_request(msg.three_pc_key, msg.stash_data)
        if params:
            self._network.send(MessageReq(**{
                f.MSG_TYPE.nm: msg.msg_type,
                f.PARAMS.nm: params
            }), dst=msg.dst)
