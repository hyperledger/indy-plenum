from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.request_handler import RequestHandler


class ReadRequestHandler(RequestHandler):
    def __init__(self):
        pass

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, request: Request):
        pass

    def get_result(self, request: Request):
        pass
