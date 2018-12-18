from abc import abstractmethod


class BatchRequestHandler:
    def __init__(self):
        pass

    @abstractmethod
    def post_apply_req_batch(self):
        pass

    @abstractmethod
    def commit_req_batch(self):
        pass

    @abstractmethod
    def revert_req_batch(self):
        pass
