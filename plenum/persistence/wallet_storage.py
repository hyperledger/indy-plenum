from abc import abstractmethod, abstractproperty

from plenum.client.id_data import IdData
from plenum.common.types import Identifier


# DEPR
# class WalletStorage:
#     @abstractmethod
#     def putIdData(self, identifier: Identifier, idData: IdData):
#         pass
#
#     @abstractmethod
#     def getIdData(self, identifier: Identifier):
#         pass
#
#     @abstractproperty
#     def identifiers(self):
#         pass
#
#     # DEPR
#     # @abstractproperty
#     # def signers(self):
#     #     pass
#     #
#     @abstractproperty
#     def aliases(self):
#         pass
