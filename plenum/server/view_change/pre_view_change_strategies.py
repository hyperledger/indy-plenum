from abc import abstractmethod, ABCMeta
from functools import partial

from plenum.common.constants import VIEW_CHANGE_START, PreVCStrategies
from plenum.common.messages.node_messages import ViewChangeStartMessage


class PreViewChangeStrategy(metaclass=ABCMeta):
    """Abstract class for routines before starting viewChange procedure"""

    def __init__(self, view_changer):
        self.view_changer = view_changer

    @abstractmethod
    def vc_preparation(self, proposed_view_no: int):
        raise NotImplementedError()


class VCStartMsgStrategy(PreViewChangeStrategy):
    """Strategy logic:
    - when startViewChange method was called, then put 'local' ViewChangeStart message
    - when this message will be processed by node's nodeInBoxRouter then it will be added to replica's inBox queue
    - when this message will be processed by replica's inBoxRouter then startViewChange method will be called
      and view_change procedure will be continued
    """

    def vc_preparation(self, proposed_view_no: int):
        self.set_req_handler()
        vcs_msg = ViewChangeStartMessage(proposed_view_no)
        nodeInBox = self.view_changer.node.nodeInBox
        nodeInBox.append((vcs_msg, self.view_changer.node.name))

    def set_req_handler(self):
        """Handler for processing ViewChangeStart message on node's nodeInBoxRouter"""
        def process_start_vc_msg_on_node(self, msg: ViewChangeStartMessage, frm):
            proposed_view_no = msg.proposed_view_no
            if proposed_view_no > self.view_changer.view_no:
                vcs_msg = ViewChangeStartMessage(proposed_view_no)
                self.view_changer.node.master_replica.inBox.append(vcs_msg)

        """Handler for processing ViewChangeStart message on replica's inBoxRouter"""
        def process_start_vc_msg_on_replica(self, msg: ViewChangeStartMessage):
            proposed_view_no = msg.proposed_view_no
            if proposed_view_no > self.node.viewNo:
                self.node.view_changer.startViewChange(proposed_view_no, continue_vc=True)

        if VIEW_CHANGE_START not in self.view_changer.node.nodeMsgRouter.routes:
            processor = partial(process_start_vc_msg_on_node,
                                self.view_changer.node)
            msg_router = self.view_changer.node.nodeMsgRouter
            msg_router.add((ViewChangeStartMessage,
                            processor))

        if VIEW_CHANGE_START not in self.view_changer.node.master_replica.inBoxRouter.routes:
            processor = partial(process_start_vc_msg_on_replica,
                                self.view_changer.node.master_replica)
            msg_router = self.view_changer.node.master_replica.inBoxRouter
            msg_router.add((ViewChangeStartMessage,
                            processor))


preVCStrategies = {
    PreVCStrategies.VC_START_MSG_STRATEGY: VCStartMsgStrategy
}
