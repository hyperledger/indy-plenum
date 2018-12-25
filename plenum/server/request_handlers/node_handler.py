from plenum.common.request import Request
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class NodeHandler(WriteRequestHandler):

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, req: Request):
        pass

    def apply_request(self, request: Request, batch_ts):
        pass

    def revert_request(self, request: Request, batch_ts):
        pass
