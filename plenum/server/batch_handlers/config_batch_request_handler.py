from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler


class ConfigBatchRequestHandler(BatchRequestHandler):
    def post_apply_req_batch(self):
        pass

    def commit_req_batch(self):
        pass

    def revert_req_batch(self):
        pass
