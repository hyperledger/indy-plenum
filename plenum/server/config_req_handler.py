from plenum.common.constants import TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML, GET_TXN_AUTHOR_AGREEMENT, \
    GET_TXN_AUTHOR_AGREEMENT_AML, TXN_TYPE, TXN_AUTHOR_AGREEMENT_VERSION, TRUSTEE
from plenum.common.exceptions import InvalidClientRequest, UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.ledger_req_handler import LedgerRequestHandler


class ConfigReqHandler(LedgerRequestHandler):
    write_types = {TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML}
    query_types = {GET_TXN_AUTHOR_AGREEMENT, GET_TXN_AUTHOR_AGREEMENT_AML}

    def __init__(self, ledger, state, domain_state):
        super().__init__(ledger, state)
        self._domain_state = domain_state

    def doStaticValidation(self, request: Request):
        pass

    def get_query_response(self, request):
        pass

    def validate(self, req: Request):
        self.authorize(req)

        operation = req.operation
        typ = operation.get(TXN_TYPE)
        if typ == TXN_AUTHOR_AGREEMENT:
            version = operation[TXN_AUTHOR_AGREEMENT_VERSION]
            if self.state.get(":taa:v:{}".format(version), isCommitted=False) is not None:
                raise InvalidClientRequest(req.identifier, req.reqId,
                                           "Changing existing version of transaction author agreement is forbidden")

    def updateState(self, txns, isCommitted=False):
        pass

    def authorize(self, req: Request):
        typ = req.operation.get(TXN_TYPE)

        if typ in [TXN_AUTHOR_AGREEMENT, TXN_AUTHOR_AGREEMENT_AML] \
                and not self.is_trustee(req.identifier):
            raise UnauthorizedClientRequest(req.identifier, req.reqId,
                                            "Only trustee can update transaction author agreement and AML")

    def is_trustee(self, nym: str):
        return bool(DomainRequestHandler.get_role(self._domain_state, nym,
                                                  TRUSTEE, isCommitted=False))
