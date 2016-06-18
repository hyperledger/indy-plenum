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
        self.keyStore = TextFileStore(self.getDataLocation(), keysFileName,
                                      storeContentHash=False)

    def addSigner(self, identifier=None, seed=None, signer=None):
        if not (seed or signer):
            error("Provide a seed or signer")
        if not signer:
            signer = SimpleSigner(identifier=identifier, seed=seed)
        if not self.getSigner(signer.identifier):
            self.keyStore.put(key=signer.identifier, value=signer.seedHex.decode())
        else:
            error("Signer already present")

    def getSigner(self, identifier) -> Optional[SimpleSigner]:
        seedHex = self.keyStore.get(identifier)
        if seedHex:
            return SimpleSigner(identifier=identifier,
                                seed=unhexlify(seedHex.encode()))

    @property
    def signers(self):
        return [SimpleSigner(identifier=identifier,
                             seed=unhexlify(seedHex.encode()))
                for identifier, seedHex in self.keyStore.iterator()]
