from abc import abstractmethod

from plenum.server.req_handlers.handler_interfaces.handler import Handler


class BatchHandler(Handler):

    @abstractmethod
    def post_batch_created(self, state_root):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def post_batch_commited(self, state_root, pp_time):
        pass

    @abstractmethod
    def post_batch_rejected(self):
        pass
