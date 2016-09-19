import os
from abc import abstractmethod
from binascii import unhexlify
from typing import Dict
from typing import Optional

from ledger.stores.text_file_store import TextFileStore
from plenum.client.id_data import IdData
from plenum.client.signer import SimpleSigner
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.types import Identifier
from plenum.common.util import error
from plenum.persistence.wallet_storage import WalletStorage


class KeyValStore:
    @abstractmethod
    def put(self, value, key):
        raise NotImplementedError

    @abstractmethod
    def get(self, key):
        raise NotImplementedError


class MemoryStore(KeyValStore):
    def __init__(self):
        self.d = {}

    def put(self, value, key):
        self.d[key] = value

    def get(self, key):
        return self.d.get(key)

    def __iter__(self):
        return self.d.items()

    def __getitem__(self, item):
        return self.d.get(item)

    def keys(self):
        return self.d.keys()


class WalletStorageMemory(WalletStorage):
    def __init__(self):
        self.ids = {}  # type: Dict[Identifier, IdData]
        self.idStore = MemoryStore()
        self.aliasesStore = MemoryStore()

    def putIdData(self, identifier: Identifier, idData: IdData):
        self.idStore.put(value=idData, key=identifier)

    def getIdData(self, identifier: Identifier):
        return self.idStore.get(identifier)

    @property
    def identifiers(self):
        return self.idStore.keys()

    @property
    def aliases(self):
        return self.aliasesStore


