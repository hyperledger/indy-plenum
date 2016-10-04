from typing import Optional, Dict

import jsonpickle
from libnacl import crypto_secretbox_open, randombytes, \
    crypto_secretbox_NONCEBYTES, crypto_secretbox

from plenum.client.signer import Signer, SimpleSigner
from plenum.common.types import Identifier, Request
from plenum.common.util import getlogger

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
                 requestIdStore: RequestIdStore = None,
                 # DEPR
                 # storage: WalletStorage
                 ):
        self._name = name
        self.idsToSigners = {}  # type: Dict[Identifier, Signer]
        self.aliasesToIds = {}  # type: Dict[Alias, Identifier]
        self.defaultId = None
        file = ""
        self._requestIdStore = requestIdStore or FileRequestIdStore(file)

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

    def addSigner(self,
                  identifier=None,
                  seed=None,
                  signer=None):
        '''
        Adds signer to the wallet.
        Requires complete signer, identifier or seed.

        :param identifier: signer identifier or None to use random one
        :param seed: signer key seed or None to use random one
        :param signer: signer to add
        :return:
        '''

        signer = signer or SimpleSigner(identifier=identifier, seed=seed)
        self.idsToSigners[signer.identifier] = signer
        if self.defaultId is None:
            # setting this signer as default signer to let use sign* methods
            # without explicit specification of signer
            self.defaultId = signer.identifier
        if signer.alias:
            self.aliasesToIds[signer.alias] = signer.identifier
        return signer

    def _requiredIdr(self,
                     idr: Identifier=None,
                     alias: str=None,
                     other: Identifier=None):
        '''
        Checks whether signer identifier specified, or can it be
        inferred from alias or can be default used instead

        :param idr:
        :param alias:
        :param other:

        :return: signer identifier
        '''

        idr = idr or other or (self.aliasesToIds[alias] if alias else self.defaultId)
        if not idr:
            raise RuntimeError('identifier required, but none found')
        return idr

    def signMsg(self,
                msg: Dict,
                identifier: Identifier=None,
                otherIdentifier: Identifier=None):
        '''
        Creates signature for message using specified signer

        :param msg: message to sign
        :param identifier: signer identifier
        :param otherIdentifier:
        :return: signature that then can be assigned to request
        '''

        idr = self._requiredIdr(idr=identifier, other=otherIdentifier)
        signer = self._signerById(idr)
        signature = signer.sign(msg)
        return signature

    def signRequest(self,
                    req: Request,
                    requestIdStore: RequestIdStore,
                    identifier: Identifier=None) -> Request:
        """
        Signs request. Modifies reqId and signature. May modify identifier.

        :param req: request
        :param requestIdStore: request id generator
        :param identifier: signer identifier
        :return: signed request
        """

        idr = self._requiredIdr(idr=identifier, other=req.identifier)
        req.identifier = idr
        req.reqId = requestIdStore.nextId()
        req.signature = self.signMsg(msg=req.getSigningState(),
                                     identifier=idr,
                                     otherIdentifier=req.identifier)
        return req

    def signOp(self,
               op: Dict,
               requestIdStore: RequestIdStore,
               identifier: Identifier=None) -> Request:
        """
        Signs the message if a signer is configured

        :param identifier: signing identifier; if not supplied the default for
            the wallet is used.
        :param op: Operation to be signed
        :return: a signed Request object
        """
        request = Request(operation=op)
        return self.signRequest(request, requestIdStore, identifier)

    # Removed:
    # _getIdData - removed in favor of passing RequestIdStore

    def _signerById(self, idr: Identifier):
        signer = self.idsToSigners.get(idr)
        if not signer:
            raise RuntimeError("No signer with idr '{}'".format(idr))
        return signer

    def _signerByAlias(self, alias: Alias):
        id = self.aliasesToIds.get(alias)
        if not id:
            raise RuntimeError("No signer with alias '{}'".format(alias))
        return self._signerById(id)

    def getVerKey(self, idr: Identifier=None) -> str:
        return self._signerById(idr or self.defaultId).verkey

    def getAlias(self, idr: Identifier):
        for alias, identifier in self.aliasesToIds.items():
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
        lst = list(self.aliasesToIds.keys())
        others = set(self.idsToSigners.keys()) - set(self.aliasesToIds.values())
        lst.extend(list(others))
        for x in exclude:
            lst.remove(x)
        return lst


class RequestIdStore:

    from abc import abstractmethod

    @abstractmethod
    def nextId(self, clientId, signerId) -> int:
        pass

    @abstractmethod
    def currentId(self, clientId, signerId) -> int:
        pass

class FileRequestIdStore(RequestIdStore):

    def __init__(self, filePath):
        self.isOpen = False
        self.storeFilePath = filePath
        self._storage = {}

    def open(self):
        if self.isOpen:
            raise RuntimeError("Storage is already open!")
        self._loadStorage()

    def close(self):
        if self.isOpen:
            self._saveStorage()

    def _loadStorage(self):
        with open(self.storeFilePath) as file:
            for line in file:
                (signerId, clientId, lastRequest) = line.split(";")
                self._storage[clientId, signerId] = int(lastRequest)

    def _saveStorage(self):
        with open(self.storeFilePath) as file:
            pass

    def __enter__(self):
        self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def nextId(self, clientId, signerId) -> int:
        lastRequestId = self._storage.get((clientId, signerId))
        if not lastRequestId:
            self._storage[signerId, signerId] = 1
            return 1
            

        return lastRequestId

    def currentId(self, clientId, signerId) -> int:
        clients = self._storage.get(signerId)
        if not clients:
            return None
