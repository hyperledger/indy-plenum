from typing import Optional, Dict, NamedTuple
import os
import stat

import jsonpickle
from libnacl import crypto_secretbox_open, randombytes, \
    crypto_secretbox_NONCEBYTES, crypto_secretbox

from plenum.common.did_method import DidMethods, DefaultDidMethods
from plenum.common.exceptions import EmptyIdentifier
from stp_core.common.log import getlogger
from stp_core.crypto.signer import Signer
from stp_core.types import Identifier
from plenum.common.request import Request
from plenum.common.util import getTimeBasedId

logger = getlogger()


class EncryptedWallet:
    def __init__(self, raw: bytes, nonce: bytes):
        self.raw = raw
        self.nonce = nonce

    def decrypt(self, key) -> 'Wallet':
        return Wallet.decrypt(self, key)


Alias = str


IdData = NamedTuple("IdData", [
    ("signer", Signer),
    ("lastReqId", int)])


class Wallet:
    def __init__(self,
                 name: str=None,
                 supportedDidMethods: DidMethods=None):
        self._name = name or 'wallet' + str(id(self))
        self.ids = {}           # type: Dict[Identifier, IdData]
        self.idsToSigners = {}  # type: Dict[Identifier, Signer]
        self.aliasesToIds = {}  # type: Dict[Alias, Identifier]
        self.defaultId = None
        self.didMethods = supportedDidMethods or DefaultDidMethods

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, newName):
        self._name = newName

    def __repr__(self):
        return self.name

    @property
    def getEnvName(self):
        return self.env if hasattr(self, "env") else None

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

    def addIdentifier(self,
                      identifier=None,
                      seed=None,
                      signer=None,
                      alias=None,
                      didMethodName=None):
        """
        Adds signer to the wallet.
        Requires complete signer, identifier or seed.

        :param identifier: signer identifier or None to use random one
        :param seed: signer key seed or None to use random one
        :param signer: signer to add
        :param alias: a friendly readable name for the signer
        :param didMethodName: name of DID Method if not the default
        :return:
        """

        dm = self.didMethods.get(didMethodName)
        signer = signer or dm.newSigner(identifier=identifier, seed=seed)
        self.idsToSigners[signer.identifier] = signer
        if self.defaultId is None:
            # setting this signer as default signer to let use sign* methods
            # without explicit specification of signer
            self.defaultId = signer.identifier
        if alias:
            signer.alias = alias
        if signer.alias:
            self.aliasesToIds[signer.alias] = signer.identifier
        return signer.identifier, signer

    def updateSigner(self, identifier, signer):
        """
        Update signer for an already present identifier. The passed signer
        should have the same identifier as `identifier` or an error is raised.
        Also if the existing identifier has an alias in the wallet then the
        passed signer is given the same alias
        :param identifier: existing identifier in the wallet
        :param signer: new signer to update too
        :return:
        """
        if identifier != signer.identifier:
            raise ValueError("Passed signer has identifier {} but it should"
                             " have been {}".format(signer.identifier,
                                                    identifier))
        if identifier not in self.idsToSigners:
            raise KeyError("Identifier {} not present in wallet".
                           format(identifier))

        oldSigner = self.idsToSigners[identifier]
        if oldSigner.alias and oldSigner.alias in self.aliasesToIds:
            logger.debug('Changing alias of passed signer to {}'.
                         format(oldSigner.alias))
            signer.alias = oldSigner.alias

        self.idsToSigners[identifier] = signer

    def requiredIdr(self,
                    idr: Identifier=None,
                    alias: str=None):
        """
        Checks whether signer identifier specified, or can it be
        inferred from alias or can be default used instead

        :param idr:
        :param alias:
        :param other:

        :return: signer identifier
        """

        # TODO Need to create a new Identifier type that supports DIDs and CIDs
        if idr:
            if ':' in idr:
                idr = idr.split(':')[1]
        else:
            idr = self.aliasesToIds[alias] if alias else self.defaultId
        if not idr:
            raise EmptyIdentifier
        return idr

    def signMsg(self,
                msg: Dict,
                identifier: Identifier=None,
                otherIdentifier: Identifier=None):
        """
        Creates signature for message using specified signer

        :param msg: message to sign
        :param identifier: signer identifier
        :param otherIdentifier:
        :return: signature that then can be assigned to request
        """

        idr = self.requiredIdr(idr=identifier or otherIdentifier)
        signer = self._signerById(idr)
        signature = signer.sign(msg)
        return signature

    def signRequest(self,
                    req: Request,
                    identifier: Identifier=None) -> Request:
        """
        Signs request. Modifies reqId and signature. May modify identifier.

        :param req: request
        :param requestIdStore: request id generator
        :param identifier: signer identifier
        :return: signed request
        """

        idr = self.requiredIdr(idr=identifier or req.identifier)
        idData = self._getIdData(idr)
        req.identifier = idr
        req.reqId = getTimeBasedId()
        req.digest = req.getDigest()
        self.ids[idr] = IdData(idData.signer, req.reqId)
        req.signature = self.signMsg(msg=req.signingState,
                                     identifier=idr,
                                     otherIdentifier=req.identifier)

        return req

    def signOp(self,
               op: Dict,
               identifier: Identifier=None) -> Request:
        """
        Signs the message if a signer is configured

        :param identifier: signing identifier; if not supplied the default for
            the wallet is used.
        :param op: Operation to be signed
        :return: a signed Request object
        """
        request = Request(operation=op)
        return self.signRequest(request, identifier)

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

    def getVerkey(self, idr: Identifier=None) -> str:
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

    # @property
    # def defaultSigner(self) -> Signer:
    #     if self.defaultId is not None:
    #         return self.idsToSigners[self.defaultId]

    def _getIdData(self,
                   idr: Identifier = None,
                   alias: Alias = None) -> IdData:
        idr = self.requiredIdr(idr, alias)
        signer = self.idsToSigners.get(idr)
        idData = self.ids.get(idr)
        return IdData(signer, idData.lastReqId if idData else None)



class WalletStorageHelper:
    """Manages wallets

    :param ``keyringsBaseDir``: keyrings base directory
    :param dmode: (optional) permissions for directories inside
        including the base one, default is 0700
    :param fmode: (optional) permissions for files inside,
        default is 0600
    """
    def __init__(self, keyringsBaseDir, dmode=0o700, fmode=0o600):
        self.dmode = dmode
        self.fmode = fmode
        self.keyringsBaseDir = keyringsBaseDir


    @property
    def keyringsBaseDir(self):
        return self._baseDir


    @keyringsBaseDir.setter
    def keyringsBaseDir(self, path):
        self._baseDir = os.path.realpath(path)
        self._baseDirParts = self._splitPath(self._baseDir)

        self._createDirIfNotExists(self._baseDir)
        self._ensurePermissions(self._baseDir, self.dmode)


    def _ensurePermissions(self, path, mode):
        if stat.S_IMODE(os.stat(path).st_mode) != mode:
            os.chmod(path, mode)


    def _createDirIfNotExists(self, dpath):
        if os.path.exists(dpath):
            if not os.path.isdir(dpath):
                raise NotADirectoryError("{}".format(dpath))
        else:
            os.makedirs(dpath)


    def _splitPath(self, path):
        parts = []

        head, tail = os.path.split(path)
        while True:
            if tail:
                parts.insert(0, tail)
                head, tail = os.path.split(head)
            else:
                if head:
                    parts.insert(0, head)
                break

        return parts


    def saveWallet(self, wallet, fpath):
        """Save wallet into specified localtion.

        Returns the canonical path for the ``fpath`` where ``wallet``
        has been stored.

        Error cases:
            - ``fpath`` is not inside the keyrings base dir - TypeError raised
            - directory part of ``fpath`` exists and it's not a directory - 
              NotADirectoryError raised
            - ``fpath`` exists and it's a directory - IsADirectoryError raised

        :param wallet: wallet to save
        :param fpath: wallet file path, absolute or relative to
            keyrings base dir
        """
        if not fpath:
            raise TypeError("empty path")

        # in case of relative path construct the absolute one
        _fpath = os.path.realpath(fpath if os.path.isabs(fpath) else 
                    os.path.join(self._baseDir, fpath))

        # splitted path of wallet directory 
        parts = self._splitPath(_fpath)[:-1]

        baseLen = len(self._baseDirParts)
        head, tail = parts[:baseLen], parts[baseLen:]

        if head != self._baseDirParts:
            raise TypeError("path {} is not insdide the keyrings {}".format(
                fpath, self._baseDir))

        if len(tail):
            self._createDirIfNotExists(os.path.join(*parts))
            # ensure permissions from the bottom of directory hierarchy
            for i in reversed(range(len(tail))):
                self._ensurePermissions(os.path.join(
                    self._baseDir, *tail[:i+1]), self.dmode)

        with open(_fpath, "w+") as wf:
            self._ensurePermissions(_fpath, self.fmode)
            encodedWallet = jsonpickle.encode(wallet, keys=True)
            wf.write(encodedWallet)

        return _fpath
