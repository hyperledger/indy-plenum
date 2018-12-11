from plenum.common.request import Request

from state.state import State

from plenum.common.ledger import Ledger

from plenum.server.pool_req_handler import PoolRequestHandler


class NodeHandler(PoolRequestHandler):
    def __init__(self, ledger: Ledger, state: State, states: dict, bls_crypto_verifier=None):
        super().__init__(ledger, state, states, bls_crypto_verifier)

    def doStaticValidation(self, request: Request):
        super().doStaticValidation(request)

    def validate(self, req: Request):
        return super().validate(req)

    def apply(self, req: Request, cons_time: int):
        return super().apply(req, cons_time)
