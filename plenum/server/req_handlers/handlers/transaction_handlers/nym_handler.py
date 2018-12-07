from plenum.server.req_handlers.handler_interfaces.request_handler import TransactionHandler


class NymHandler(TransactionHandler):
    def static_validation(self, identifier, req_id, operation):
        error = None
        if not self.isSteward(self.state,
                              identifier, isCommitted=False):
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

    def dynamic_validation(self, request):
        pass

    def apply_txn(self, txn, is_committed):
        pass
