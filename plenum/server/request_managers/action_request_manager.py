from typing import Dict, List

from plenum.server.request_handlers.handler_interfaces.action_request_handler import ActionRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ActionRequestManager(RequestManager):
    def __init__(self):
        self.request_handlers = {}  # type: Dict[int,List[ActionRequestHandler]]

    def process_action(self):
        pass
