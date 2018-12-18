from plenum.common.constants import POOL_LEDGER_ID, NODE, DATA, BLS_KEY, BLS_KEY_PROOF
from plenum.common.exceptions import InvalidClientRequest
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class NodeHandler(WriteRequestHandler):

    def __init__(self, config, database_manager: DatabaseManager, bls_crypto_verifier):
        super().__init__(config, database_manager, NODE, POOL_LEDGER_ID)
        self.bls_crypto_verifier = bls_crypto_verifier

    def static_validation(self, request: Request):
        if request.txn_type != NODE:
            return
        blskey = request.operation.get(DATA).get(BLS_KEY, None)
        blskey_proof = request.operation.get(DATA).get(BLS_KEY_PROOF, None)
        if blskey is None and blskey_proof is None:
            return
        if blskey is None and blskey_proof is not None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "A Proof of possession is not "
                                       "needed without BLS key")
        if blskey is not None and blskey_proof is None:
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "A Proof of possession must be "
                                       "provided with BLS key")
        if not self._verify_bls_key_proof_of_possession(blskey_proof,
                                                        blskey):
            raise InvalidClientRequest(request.identifier, request.reqId,
                                       "Proof of possession {} is incorrect "
                                       "for BLS key {}".
                                       format(blskey_proof, blskey))

    def dynamic_validation(self, req: Request):
        pass

    def apply_request(self, request: Request, batch_ts):
        pass

    def revert_request(self, request: Request, batch_ts):
        pass

    def _verify_bls_key_proof_of_possession(self, key_proof, pk):
        return True if self.bls_crypto_verifier is None else \
            self.bls_crypto_verifier.verify_key_proof_of_possession(key_proof,
                                                                    pk)
