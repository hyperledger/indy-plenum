from abc import abstractmethod, ABCMeta
from collections import deque
from functools import partial

from plenum.common.constants import VIEW_CHANGE_START, PreVCStrategies, VIEW_CHANGE_CONTINUE
from plenum.common.messages.node_messages import ViewChangeStartMessage, ViewChangeContinueMessage, PrePrepare, Prepare, \
    Commit, Ordered
from stp_zmq.zstack import Quota
from stp_core.common.log import getlogger


logger = getlogger()


class PreViewChangeStrategy(metaclass=ABCMeta):
    """Abstract class for routines before starting viewChange procedure"""

    def __init__(self, view_changer, node):
        self.view_changer = view_changer
        self.node = node

    @abstractmethod
    def prepare_view_change(self, proposed_view_no: int):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def on_view_change_started(obj, msg, frm):
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def on_view_change_continued(obj, msg):
        raise NotImplementedError()

    @abstractmethod
    def on_strategy_complete(self):
        raise NotImplementedError()


class VCStartMsgStrategy(PreViewChangeStrategy):
    """Strategy logic:
    - when start_view_change method was called, then put 'local' ViewChangeStart message and set corresponded handlers
    - on processing start_view_change message on the nodeInBoxRouter's side the next steps will be performed:
        - call nodestack.service method with extended quota parameters for getting as much as possible 3PC
          messages from ZMQ's side
        - process all messages from nodeInBox queue and stash all not 3PC
        - append to replica's inBox queue ViewChangeContinueMessage
    - then replica's inBox queue will be processed and after ViewChangeContinueMessage view_change procedure
      will be continued in the normal way
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stashedNodeInBox = deque()
        self.replica = self.node.master_replica
        self.is_preparing = False

    def prepare_view_change(self, proposed_view_no: int):
        if not self.is_preparing:
            logger.info("VCStartMsgStrategy: Starting prepare_view_change process")
            self._set_req_handlers()
            vcs_msg = ViewChangeStartMessage(proposed_view_no)
            nodeInBox = self.node.nodeInBox
            nodeInBox.append((vcs_msg, self.node.name))
            self.is_preparing = True

    def on_strategy_complete(self):
        logger.info("VCStartMsgStrategy: on_strategy_complete - View Change can be started")
        self.unstash_messages()
        self.is_preparing = False

    @staticmethod
    async def _process_node_inbox_3PC(node):
        current_view_no = node.viewNo
        stashed_not_3PC = deque()
        types_3PC = (PrePrepare, Prepare, Commit, Ordered)
        while node.nodeInBox:
            m = node.nodeInBox.popleft()
            if len(m) == 2 and isinstance(m[0], types_3PC) and \
                m[0].viewNo == current_view_no and \
                    m[0].instId == node.instances.masterId:
                await node.process_one_node_message(m)
            else:
                stashed_not_3PC.append(m)
        return stashed_not_3PC

    """Handler for processing ViewChangeStart message on node's nodeInBoxRouter"""
    @staticmethod
    async def on_view_change_started(node, msg: ViewChangeStartMessage, frm):
        strategy = node.view_changer.pre_vc_strategy
        proposed_view_no = msg.proposed_view_no
        logger.info("VCStartMsgStrategy: got ViewChangeStartMessage with proposed_view_no: {}".format(proposed_view_no))
        if proposed_view_no > node.view_changer.view_no:
            vcc_msg = ViewChangeContinueMessage(proposed_view_no)
            quota = Quota(
                count=node.config.EXTENDED_QUOTA_MULTIPLIER_BEFORE_VC * node.quota_control.node_quota.count,
                size=node.config.EXTENDED_QUOTA_MULTIPLIER_BEFORE_VC * node.quota_control.node_quota.size)
            msgs_count = await node.nodestack.service(limit=None,
                                                      quota=quota)
            logger.info("VCStartMsgStrategy: Got {} messages from nodestack".format(msgs_count))
            strategy.stashedNodeInBox = await VCStartMsgStrategy._process_node_inbox_3PC(node)
            logger.info("VCStartMsgStrategy: {} not 3PC msgs was stashed".format(len(strategy.stashedNodeInBox)))
            node.master_replica.inBox.append(vcc_msg)

    """Handler for processing ViewChangeStart message on replica's inBoxRouter"""
    @staticmethod
    def on_view_change_continued(replica, msg: ViewChangeContinueMessage):
        strategy = replica.node.view_changer.pre_vc_strategy
        proposed_view_no = msg.proposed_view_no
        replica.logger.info("VCStartMsgStrategy: got ViewChangeContinueMessage with proposed_view_no: {}".format(proposed_view_no))
        if proposed_view_no > replica.node.viewNo:
            """
            Return stashed not 3PC msgs to nodeInBox queue and start ViewChange
            Critical assumption: All 3PC msgs passed from node already processed
            """
            strategy.unstash_messages()
            replica.logger.info("VCStartMsgStrategy: continue view_change procedure in a normal way")
            replica.node.view_changer.start_view_change(proposed_view_no, continue_vc=True)
            strategy.is_preparing = False

    def unstash_messages(self):
        logger.info("VCStartMsgStrategy: unstash all not 3PC msgs to nodeInBox queue")
        while self.stashedNodeInBox:
            self.node.nodeInBox.appendleft(self.stashedNodeInBox.pop())

    def _set_req_handlers(self):
        node_msg_router = self.node.nodeMsgRouter
        replica_msg_router = self.replica.inBoxRouter

        if ViewChangeStartMessage not in node_msg_router.routes:
            processor = partial(VCStartMsgStrategy.on_view_change_started,
                                self.node)
            node_msg_router.add((ViewChangeStartMessage, processor))

        if ViewChangeContinueMessage not in replica_msg_router.routes:
            processor = partial(VCStartMsgStrategy.on_view_change_continued,
                                self.replica)
            replica_msg_router.add((ViewChangeContinueMessage, processor))


preVCStrategies = {
    PreVCStrategies.VC_START_MSG_STRATEGY: VCStartMsgStrategy
}
