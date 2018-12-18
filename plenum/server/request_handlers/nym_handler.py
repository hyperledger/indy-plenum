from plenum.common.constants import NYM, ROLE, STEWARD, DOMAIN_LEDGER_ID
from plenum.common.exceptions import UnauthorizedClientRequest
from plenum.common.request import Request
from plenum.server.database_manager import DatabaseManager
from plenum.server.request_handlers.handler_interfaces.write_request_handler import WriteRequestHandler


class NymHandler(WriteRequestHandler):
    def __init__(self, database_manager: DatabaseManager):
        super().__init__(database_manager, NYM)

    def static_validation(self, request: Request):
        pass

    def dynamic_validation(self, req: Request):
        origin = req.identifier
        error = None
        if not self.isSteward(self.state,
                              origin, isCommitted=False):
            error = "Only Steward is allowed to do these transactions"
        if req.operation.get(ROLE) == STEWARD:
            if self.stewardThresholdExceeded(self.config):
                error = "New stewards cannot be added by other stewards " \
                        "as there are already {} stewards in the system". \
                    format(self.config.stewardThreshold)
        if error:
            raise UnauthorizedClientRequest(req.identifier,
                                            req.reqId,
                                            error)

    def apply_request(self, request: Request, batch_ts):
        pass

    def revert_request(self, request: Request, batch_ts):
        pass

    @property
    def state(self):
        self.database_manager.get_database(DOMAIN_LEDGER_ID)
