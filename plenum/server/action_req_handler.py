from plenum.common.request import Request
from plenum.server.req_handler import RequestHandler


class ActionReqHandler(RequestHandler):
    def __init__(self):
        super().__init__()

    def doStaticValidation(self, request: Request):
        pass

    def validate(self, req: Request):
        pass

    def apply(self, req: Request, cons_time: int=None):
        pass
