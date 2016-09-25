from typing import Optional, Dict

import jsonpickle
from libnacl import crypto_secretbox_open, randombytes, \
    crypto_secretbox_NONCEBYTES, crypto_secretbox

from plenum.client.id_data import IdData
from plenum.client.signer import SimpleSigner
from plenum.common.util import getlogger
# from plenum.persistence.wallet_storage import WalletStorage
from plenum.common.types import f, Identifier, Request

logger = getlogger()


class EncryptedWallet:
    def __init__(self, raw: bytes, nonce: bytes):
        self.raw = raw
        self.nonce = nonce

    def decrypt(self, key) -> 'Wallet':
        return Wallet.decrypt(self, key)


Alias = str


class Wallet:
    def __init__(self,
                 name: str,
                 # DEPR
                 # storage: WalletStorage
                 ):
        self._name = name
        self.ids = {}  # type: Dict[Identifier, IdData]
        self.aliases = {}  # type: Dict[Alias, Identifier]
        self.defaultId = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, newName):
        self._name = newName

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

    def addSigner(self, identifier=None, seed=None, signer=None):
        if not signer:
            signer = SimpleSigner(identifier=identifier, seed=seed)
        idr = signer.identifier
        if self.defaultId is None:
            self.defaultId = idr
        self.ids[idr] = IdData(signer=signer)
        if signer.alias:
            self.aliases[signer.alias] = signer.identifier
        return signer

    def _requiredIdr(self, idr: Identifier=None, alias: str=None,
                     other: Identifier=None):
        idr = idr or other or (
            self.aliases[alias] if alias else self.defaultId)
        if not idr:
            raise RuntimeError('identifier required, but none found')
        return idr

    def signRequest(self, req: Request, identifier: Identifier=None) -> Request:
        """
        Signs request. Modifies reqId and signature. May modify identifier.
        :param req:
        :param identifier:
        :return:
        """
        idr = self._requiredIdr(idr=identifier, other=req.identifier)
        req.identifier = idr
        idData = self._getIdData(idr)
        req.reqId = idData.lastReqId + 1
        if idData.signer:
            req.signature = idData.signer.sign(req.getSigningState())
            idData.lastReqId += 1
            self.ids[idr] = idData
        else:
            raise RuntimeError('{} signer not configured so cannot sign '
                               '{}'.format(self, req))
        return req

    def signOp(self, op: Dict, identifier: Identifier=None) -> Request:
        """
        Signs the message if a signer is configured

        :param identifier: signing identifier; if not supplied the default for
            the wallet is used.
        :param op: Operation to be signed
        :return: a signed Request object
        """
        return self.signRequest(Request(operation=op), identifier=identifier)

    def _getIdData(self,
                   idr: Identifier=None,
                   alias: Alias=None) -> IdData:
        idr = self._requiredIdr(idr, alias)
        return self.ids.get(idr, None)
        # DEPR
        # return self.storage.getSigner(identifier=identifier, alias=alias)

    def getVerKey(self, idr: Identifier=None) -> str:
        data = self._getIdData(idr)
        return data.signer.verkey

    def getAlias(self, idr: Identifier):
        for alias, identifier in self.aliases.items():
            if identifier == idr:
                return alias

    @property
    def identifiers(self):
        return self.listIds()

    @property
    def defaultAlias(self):
        return self.getAlias(self.defaultId)

    def listIds(self, exclude=list()):
        """
        For each signer in this wallet, return its alias if present else
        return its identifier.

        :param exclude:
        :return: List of identifiers/aliases.
        """
        lst = list(self.aliases.keys())
        others = set(self.ids.keys()) - set(self.aliases.values())
        lst.extend(list(others))
        for x in exclude:
            lst.remove(x)
        return lst
