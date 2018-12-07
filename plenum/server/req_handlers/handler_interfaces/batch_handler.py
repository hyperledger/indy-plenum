from abc import abstractmethod, ABC


class BatchHandler(ABC):

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
