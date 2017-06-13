from abc import abstractmethod
from typing import Optional

from plenum.common.types import LedgerStatus, ConsistencyProof, ConsProofRequest, CatchupReq, CatchupRep, PrePrepare, \
    Prepare, Commit, Ordered
from plenum.server.msg_filter import MessageFilter


class ViewChangeMessageFilter(MessageFilter):

    NAME = "ViewChangeMessageFilter"

    def __init__(self, view_no):
        self.__view_no = view_no

    def filter(self, msg) -> Optional[str]:
        if self.__is_catch_up_msg(msg):
            return None

        if self.__is_same_view_3pc_msg(msg):
            return None

        return 'Message is neither catch-up nor 3PC message for the current view'


    def __is_catch_up_msg(self, msg):
        catch_up_msgs = [LedgerStatus,
                         ConsistencyProof,
                         ConsProofRequest,
                         CatchupReq,
                         CatchupRep]

        for catch_up_msg in catch_up_msgs:
            if isinstance(msg, catch_up_msg):
                return True

        return False

    def __is_same_view_3pc_msg(self, msg):
        msgs_3pc = [PrePrepare,
                    Prepare,
                    Commit,
                    Ordered]

        for msg_3pc in msgs_3pc:
            if isinstance(msg, msg_3pc) and\
                            msg.viewNo <= self.__view_no:
                return True

        return False

