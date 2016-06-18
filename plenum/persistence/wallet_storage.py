from abc import abstractmethod, abstractproperty


class WalletStorage:
    @abstractmethod
    def addSigner(self, identifier=None, seed=None, signer=None, alias=None):
        pass

    @abstractmethod
    def getSigner(self, identifier=None, alias=None):
        pass

    @abstractproperty
    def signers(self):
        pass

    @abstractproperty
    def aliases(self):
        pass
