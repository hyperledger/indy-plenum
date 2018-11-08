from abc import abstractmethod, ABCMeta
from collections import deque
from functools import partial

from plenum.common.constants import VIEW_CHANGE_START, PreVCStrategies, VIEW_CHANGE_CONTINUE
from plenum.common.messages.node_messages import ViewChangeStartMessage, ViewChangeContinueMessage, PrePrepare, Prepare, \
    Commit, Ordered
from stp_zmq.zstack import Quota


class PreViewChangeStrategy(metaclass=ABCMeta):
    """Abstract class for routines before starting viewChange procedure"""

    def __init__(self, view_changer):
        self.view_changer = view_changer

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


class VCStartMsgStrategy(PreViewChangeStrategy):
    """Strategy logic:
    - when startViewChange method was called, then put 'local' ViewChangeStart message
    - when this message will be processed by node's nodeInBoxRouter then it will be added to replica's inBox queue
    - when this message will be processed by replica's inBoxRouter then startViewChange method will be called
      and view_change procedure will be continued
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stashedNodeInBox = deque()
        self.node = self.view_changer.node
        self.replica = self.node.master_replica
        self.is_preparing = False

    def prepare_view_change(self, proposed_view_no: int):
        if not self.is_preparing:
            self._set_req_handlers()
            vcs_msg = ViewChangeStartMessage(proposed_view_no)
            nodeInBox = self.view_changer.node.nodeInBox
            nodeInBox.append((vcs_msg, self.view_changer.node.name))
            self.is_preparing = True

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
        if proposed_view_no > node.view_changer.view_no:
            vcc_msg = ViewChangeContinueMessage(proposed_view_no)
            quota = Quota(
                count=node.config.EXTENDED_QUOTA_MULTIPLIER_BEFORE_VC * node.quota_control.node_quota.count,
                size=node.config.EXTENDED_QUOTA_MULTIPLIER_BEFORE_VC * node.quota_control.node_quota.size)
            await node.nodestack.service(limit=None,
                                         quota=quota)
            strategy.stashedNodeInBox = await VCStartMsgStrategy._process_node_inbox_3PC(node)
            node.master_replica.inBox.append(vcc_msg)

    """Handler for processing ViewChangeStart message on replica's inBoxRouter"""
    @staticmethod
    def on_view_change_continued(replica, msg: ViewChangeContinueMessage):
        strategy = replica.node.view_changer.pre_vc_strategy
        proposed_view_no = msg.proposed_view_no
        if proposed_view_no > replica.node.viewNo:
            """
            Return stashed not 3PC msgs to nodeInBox queue and start ViewChange
            Critical assumption: All 3PC msgs passed from node already processed
            """
            while strategy.stashedNodeInBox:
                replica.node.nodeInBox.appendleft(strategy.stashedNodeInBox.pop())
            replica.node.view_changer.startViewChange(proposed_view_no, continue_vc=True)
            strategy.is_preparing = False

    def _set_req_handlers(self):
        node_msg_router = self.node.nodeMsgRouter
        replica_msg_router = self.replica.inBoxRouter

        if VIEW_CHANGE_START not in node_msg_router.routes:
            processor = partial(VCStartMsgStrategy.on_view_change_started,
                                self.node)
            node_msg_router.add((ViewChangeStartMessage, processor))

        if VIEW_CHANGE_CONTINUE not in replica_msg_router.routes:
            processor = partial(VCStartMsgStrategy.on_view_change_continued,
                                self.replica)
            replica_msg_router.add((ViewChangeContinueMessage, processor))


preVCStrategies = {
    PreVCStrategies.VC_START_MSG_STRATEGY: VCStartMsgStrategy
}
