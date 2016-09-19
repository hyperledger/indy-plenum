import os
from binascii import unhexlify
from typing import Optional

from ledger.stores.text_file_store import TextFileStore
from plenum.client.id_data import IdData
from plenum.client.signer import SimpleSigner, Signer
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.types import Identifier
from plenum.common.util import error
from plenum.persistence.wallet_storage import WalletStorage
import jsonpickle


class WalletStorageFile(WalletStorage, HasFileStorage):

    def __init__(self, walletDir: str):
        HasFileStorage.__init__(self, name="wallet", baseDir=walletDir)
        keysFileName = "keys"
        aliasesFileName = "aliases"
        self.idStore = TextFileStore(self.dataLocation, keysFileName,
                                     storeContentHash=False)
        self.aliasesStore = TextFileStore(self.dataLocation,
                                          aliasesFileName,
                                          storeContentHash=False)

    @classmethod
    def fromName(cls, name, basepath):
        path = cls.path(name, basepath)
        return cls(path)

    @classmethod
    def path(cls, name, basepath):
        return os.path.join(basepath, "data", "clients", name)

    @classmethod
    def path(cls, name, basepath):
        pathparts = [basepath, "data", "clients"]
        if name:
            pathparts.append(name)
        return os.path.join(*pathparts)

    @classmethod
    def exists(cls, name, basepath):
        return os.path.exists(cls.path(name, basepath))

    @classmethod
    def listWallets(cls, basepath):
        p = cls.path(None, basepath)
        ls = os.listdir(p) if os.path.isdir(p) else []
        return [name for name in ls
                if os.path.isdir(os.path.join(p, name))]

    def putIdData(self, identifier: Identifier, idData: IdData):
        self.idStore.put(key=identifier,
                         value=jsonpickle.encode(idData.__getstate__()))
        if idData.signer.alias:
            self.aliasesStore.put(key=idData.signer.alias, value=identifier)

    def getIdData(self, identifier: Identifier) -> Optional[IdData]:
        encoded = self.idStore.get(identifier)
        if encoded:
            decoded = jsonpickle.decode(encoded)
            idData = IdData()
            idData.__setstate__(decoded)
            return idData

    @property
    def identifiers(self):
        return [idr for idr in self.idStore.iterator(includeValue=False)]

    # DEPR
    # @property
    # def signers(self):
    #     signers = {identifier: SimpleSigner(identifier=identifier,
    #                                         seed=unhexlify(seedHex.encode()))
    #                for identifier, seedHex in self.idStore.iterator()}
    #     aliases = {}
    #     for alias, identifier in self.aliasesStore.iterator():
    #         aliases[identifier] = alias
    #     return [(signer, aliases.get(idf)) for idf, signer in signers.items()]
    #
    @property
    def aliases(self):
        return self.aliasesStore.iterator()


# import pickledb
#
# class A:
#     def __init__(self):
#         self.a = 1
#         self.b = 2
#
#     def __getstate__(self):
#         return self.__dict__
#
#
# db = pickledb.load('test.db', False)
# db.set('key3', A())
# db.get('key')
# db.dump()
