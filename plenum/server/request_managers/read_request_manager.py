from typing import Dict

from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_managers.request_manager import RequestManager


class ReadRequestManager(RequestManager):
    def __init__(self):
        self.request_handlers = {}  # type: Dict[int,ReadRequestHandler]

    def get_result(self):
        pass
