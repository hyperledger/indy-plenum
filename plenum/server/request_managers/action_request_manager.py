from typing import Dict, List

from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.action_request_handler import ActionRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ActionRequestManager(RequestManager):
    def __init__(self):
        self.request_handlers = {}  # type: Dict[int, ActionRequestHandler]

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def process_action(self, request: Request):
        pass
