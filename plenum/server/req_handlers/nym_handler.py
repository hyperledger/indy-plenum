from requests import Request

from plenum.server.domain_req_handler import DomainRequestHandler


class NymHandler(DomainRequestHandler):

    def __init__(self, ledger, state, config, reqProcessors, bls_store, ts_store=None):
        super().__init__(ledger, state, config, reqProcessors, bls_store, ts_store)

    def doStaticValidation(self, request: Request):
        super().doStaticValidation(request)

    def validate(self, req: Request):
        return super().validate(req)

    def apply(self, req: Request, cons_time: int):
        return super().apply(req, cons_time)
