from abc import abstractmethod, ABC


class TransactionHandler(ABC):

    @abstractmethod
    def static_validation(self, identifier, req_id, operation):
        pass

    @abstractmethod
    def dynamic_validation(self, request):
        pass

    @abstractmethod
    def apply_txn(self, txn, is_committed):
        pass
