from abc import ABC, abstractmethod


class HandlerBuilder(ABC):

    @abstractmethod
    def build_action_handlers(self):
        pass

    @abstractmethod
    def build_batch_handlers(self):
        pass

    @abstractmethod
    def build_query_handlers(self):
        pass

    @abstractmethod
    def build_transaction_handlers(self):
        pass
