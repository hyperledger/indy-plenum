from plenum.server.batch_handlers.batch_request_handler import BatchRequestHandler


class PoolBatchHandler(BatchRequestHandler):
    def post_apply_batch(self):
        pass

    def commit_batch(self):
        pass

    def revert_batch(self):
        pass
