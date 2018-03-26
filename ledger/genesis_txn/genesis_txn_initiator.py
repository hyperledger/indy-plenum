from abc import ABCMeta, abstractmethod


class GenesisTxnInitiator(metaclass=ABCMeta):
    @abstractmethod
    def init_ledger_from_genesis_txn(self, ledger):
        pass

    # TODO: think about a factory for GenesisTxnInitiator
    @abstractmethod
    def create_initiator_ledger(self):
        pass
