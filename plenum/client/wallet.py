from typing import Optional

import jsonpickle
from libnacl import crypto_secretbox_open, randombytes, \
    crypto_secretbox_NONCEBYTES, crypto_secretbox

from plenum.client.signer import Signer
from plenum.persistence.wallet_storage import WalletStorage


class EncryptedWallet:
    def __init__(self, raw: bytes, nonce: bytes):
        self.raw = raw
        self.nonce = nonce

    def decrypt(self, key) -> 'Wallet':
        return Wallet.decrypt(self, key)


class Wallet:
    def __init__(self, storage: WalletStorage):
        self.signers = {}
        self.aliases = {}
        self.storage = storage
        for (signer, alias) in self.storage.signers:
            if alias:
                self.aliases[alias] = signer.identifier
            self.signers[signer.identifier] = signer

    @staticmethod
    def decrypt(ec: EncryptedWallet, key: bytes) -> 'Wallet':
        decryped = crypto_secretbox_open(ec.raw, ec.nonce, key)
        decoded = decryped.decode()
        wallet = jsonpickle.decode(decoded)
        return wallet

    def encrypt(self, key: bytes,
                nonce: Optional[bytes] = None) -> EncryptedWallet:
        serialized = jsonpickle.encode(self)
        byts = serialized.encode()
        nonce = nonce if nonce else randombytes(crypto_secretbox_NONCEBYTES)
        raw = crypto_secretbox(byts, nonce, key)
        return EncryptedWallet(raw, nonce)

    def addSigner(self, signer: Signer, alias=None):
        self.signers[signer.identifier] = signer
        self.storage.addSigner(signer=signer, alias=alias)
        if alias:
            self.aliases[alias] = signer.identifier

    def getSigner(self, identifier=None, alias=None):
        return self.storage.getSigner(identifier=identifier, alias=alias)

    def listIds(self):
        """
        For each signer in this wallet, return its alias if present else
        return its identifier.

        :return: List of identifiers/aliases.
        """
        lst = list(self.aliases.keys())
        others = set(self.signers.keys()) - set(self.aliases.values())
        lst.extend(list(others))
        return lst
