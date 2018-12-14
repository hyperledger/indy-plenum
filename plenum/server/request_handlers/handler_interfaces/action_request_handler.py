from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class ActionRequestHandler(RequestHandler):
    def __init__(self):
        pass

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def process_action(self, request: Request):
        pass
