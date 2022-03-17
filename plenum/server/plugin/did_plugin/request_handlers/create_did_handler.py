from typing import Optional

from plenum.common.constants import DATA
from plenum.common.request import Request
from common.serializers.serialization import domain_state_serializer

from plenum.server.database_manager import DatabaseManager
from plenum.server.plugin.did_plugin.constants import CREATE_DID
from plenum.server.plugin.did_plugin.request_handlers.abstract_did_req_handler import AbstractDIDReqHandler

from plenum.common.txn_util import get_payload_data, get_from, \
    get_seq_no, get_txn_time, get_request_data

class CreateDIDHandler(AbstractDIDReqHandler):

    def __init__(self, database_manager: DatabaseManager, did_dict: dict):
        super().__init__(database_manager, CREATE_DID, did_dict)

    def additional_dynamic_validation(self, request: Request, req_pp_time: Optional[int]):
        operation = request.operation
        data = operation.get(DATA)
        self.did_dict[data['id']] = {'bishakh':'success'}

    def update_state(self, txn, prev_result, request, is_committed=False):
        data = get_payload_data(txn).get(DATA)
        self.did_dict.setdefault(data["id"], {})
        self.did_dict[data["id"]]["dummykey"] = "dummyval"
        key = data["id"]
        val = self.did_dict[data["id"]]
        print("Setting state:", key, val)
        self.state.set(key.encode(), domain_state_serializer.serialize(val))
        return val
