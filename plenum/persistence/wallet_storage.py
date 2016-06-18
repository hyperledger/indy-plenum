from abc import abstractmethod, abstractproperty


class WalletStorage:
    @abstractmethod
    def addSigner(self, seed=None, signer=None):
        pass

    @abstractmethod
    def getSigner(self, identifier):
        pass

    @abstractproperty
    def signers(self):
        pass
