from plenum.common.constants import GET_FROZEN_LEDGERS, CONFIG_LEDGER_ID
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.read_request_handler import ReadRequestHandler
from plenum.server.request_handlers.ledgers_freeze.ledger_freeze_helper import StaticLedgersFreezeHelper


class GetFrozenLedgersHandler(ReadRequestHandler):

    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, GET_FROZEN_LEDGERS, CONFIG_LEDGER_ID)

    def static_validation(self, request: Request):
        self._validate_request_type(request)

    def get_result(self, request: Request):
        self._validate_request_type(request)
        state_path = StaticLedgersFreezeHelper.make_state_path_for_frozen_ledgers()
        try:
            data, last_seq_no, last_update_time, proof = self.lookup(state_path, is_committed=True, with_proof=True)
        except KeyError:
            data, last_seq_no, last_update_time, proof = None, None, None, None
        result = self.make_result(request=request,
                                  data=data,
                                  last_seq_no=last_seq_no,
                                  update_time=last_update_time,
                                  proof=proof)
        return result
