from plenum.common.request import Request

from plenum.server.req_handler import RequestHandler


class QueryRequestHandler(RequestHandler):
    def __init__(self):
        super().__init__()

    def doStaticValidation(self, request: Request):
        super().doStaticValidation(request)

    def validate(self, req: Request):
        super().validate(req)

    def apply(self, req: Request, cons_time: int):
        super().apply(req, cons_time)
