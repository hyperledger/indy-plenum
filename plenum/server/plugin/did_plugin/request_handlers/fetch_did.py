from plenum.common.constants import DATA

from common.serializers.json_serializer import JsonSerializer
from plenum.common.request import Request
from plenum.common.types import f
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.plugin.did_plugin import DID_PLUGIN_LEDGER_ID
from plenum.server.plugin.did_plugin.constants import FETCH_DID


class FetchDIDHandler(ReadRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, FETCH_DID, DID_PLUGIN_LEDGER_ID)

    def static_validation(self, request: Request):
        pass

    def get_result(self, request: Request):
        did = request.operation.get(DATA).get("id").encode()
        serialized_did = self.state.get(did, isCommitted=True)
        did_data, proof = self._get_value_from_state(did, with_proof=True)
        
        did = JsonSerializer().deserialize(serialized_did) if serialized_did else None

        return {**request.operation, **{
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
            "did": did
        }}
