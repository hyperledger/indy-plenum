from typing import Optional

from plenum.common.request import Request
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.server.msg_filter import MessageFilter


class ViewChangeMessageFilter(MessageFilter):

    NAME = "ViewChangeMessageFilter"

    def __init__(self, view_no):
        self.__view_no = view_no

    def filter_node_to_node(self, msg) -> Optional[str]:
        if self.__is_next_view_3pc_msg(msg):
            return 'A message for the next view'

        return None

    def filter_client_to_node(self, req) -> Optional[str]:
        if isinstance(req, Request):
            return 'Can not process requests when view change is in progress'

        return None

    def __is_next_view_3pc_msg(self, msg):
        msgs_3pc = [PrePrepare,
                    Prepare,
                    Commit]

        for msg_3pc in msgs_3pc:
            if isinstance(msg, msg_3pc) and\
                    msg.viewNo > self.__view_no:
                return True

        return False
