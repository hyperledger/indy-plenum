from binascii import unhexlify
from typing import Optional

from ledger.stores.text_file_store import TextFileStore
from plenum.client.signer import SimpleSigner
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.util import error
from plenum.persistence.wallet_storage import WalletStorage


class WalletStorageFile(WalletStorage, HasFileStorage):
    def __init__(self, walletDir: str):
        HasFileStorage.__init__(self, name="wallet", baseDir=walletDir)
        keysFileName = "keys"
        aliasesFileName = "aliases"
        self.keyStore = TextFileStore(self.getDataLocation(), keysFileName,
                                      storeContentHash=False)
        self.aliasesStore = TextFileStore(self.getDataLocation(),
                                          aliasesFileName,
                                          storeContentHash=False)

    def addSigner(self, identifier=None, seed=None, signer=None, alias=None):
        if not (seed or signer):
            error("Provide a seed or signer")
        if not signer:
            signer = SimpleSigner(identifier=identifier, seed=seed)
        identifier = signer.identifier
        if not self.getSigner(identifier):
            self.keyStore.put(key=identifier, value=signer.seedHex.decode())
            if alias:
                self.aliasesStore.put(key=alias, value=identifier)
        else:
            error("Signer already present")

    def getSigner(self, identifier=None, alias=None) -> Optional[SimpleSigner]:
        if alias:
            identifier = self.aliasesStore.get(key=alias)

        if identifier:
            seedHex = self.keyStore.get(identifier)
            if seedHex:
                return SimpleSigner(identifier=identifier,
                                    seed=unhexlify(seedHex.encode()))

    @property
    def signers(self):
        signers = {identifier: SimpleSigner(identifier=identifier,
                                            seed=unhexlify(seedHex.encode()))
                   for identifier, seedHex in self.keyStore.iterator()}
        aliases = {}
        for alias, identifier in self.aliasesStore.iterator():
            aliases[identifier] = alias
        return [(signer, aliases.get(idf)) for idf, signer in signers.items()]

    @property
    def aliases(self):
        return self.aliasesStore.iterator()