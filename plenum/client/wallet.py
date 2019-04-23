from typing import Optional, Dict, NamedTuple
import os
import sys
import stat
from pathlib import Path

import jsonpickle
from jsonpickle import JSONBackend
from libnacl import crypto_secretbox_open, randombytes, \
    crypto_secretbox_NONCEBYTES, crypto_secretbox
from plenum.common.constants import CURRENT_PROTOCOL_VERSION

from plenum.common.did_method import DidMethods, DefaultDidMethods
from plenum.common.exceptions import EmptyIdentifier
from plenum.common.util import lxor
from stp_core.common.log import getlogger
from stp_core.crypto.signer import Signer
from stp_core.types import Identifier
from plenum.common.request import Request

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

        idr = self.requiredIdr(idr=identifier or req._identifier)
        # idData = self._getIdData(idr)
        req._identifier = idr
        req.reqId = req.gen_req_id()
        # req.digest = req.getDigest()
        # QUESTION: `self.ids[idr]` would be overwritten if same identifier
        # is used to send 2 requests, why is `IdData` persisted?
        # self.ids[idr] = IdData(idData.signer, req.reqId)
        req.signature = self.signMsg(msg=req.signingPayloadState(identifier=idr),
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
        request = Request(operation=op,
                          protocolVersion=CURRENT_PROTOCOL_VERSION)
        return self.signRequest(request, identifier)

    def do_multi_sig_on_req(self, request: Request, identifier: str):
        idr = self.requiredIdr(idr=identifier)
        signature = self.signMsg(msg=request.signingPayloadState(identifier),
                                 identifier=idr)
        request.add_signature(idr, signature)

    def sign_using_multi_sig(self, op: Dict=None, request: Request=None,
                             identifier=None) -> Request:
        # One and only 1 of `op` and `request` must be provided.
        # If `request` is provided it must have `reqId`
        assert lxor(op, request)
        identifier = identifier or self.defaultId
        if op:
            request = Request(reqId=Request.gen_req_id(), operation=op,
                              protocolVersion=CURRENT_PROTOCOL_VERSION)
        self.do_multi_sig_on_req(request, identifier)
        return request

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
        others = set(self.idsToSigners.keys()) - \
            set(self.aliasesToIds.values())
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
        return str(self._baseDir)

    @keyringsBaseDir.setter
    def keyringsBaseDir(self, path):
        self._baseDir = self._resolve(Path(path))

        self._createDirIfNotExists(self._baseDir)
        self._ensurePermissions(self._baseDir, self.dmode)

    def _ensurePermissions(self, path, mode):
        if stat.S_IMODE(path.stat().st_mode) != mode:
            path.chmod(mode)

    def _createDirIfNotExists(self, dpath):
        if dpath.exists():
            if not dpath.is_dir():
                raise NotADirectoryError("{}".format(dpath))
        else:
            dpath.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path):
        # ``strict`` argument appeared only version 3.6 of python
        if sys.version_info < (3, 6, 0):
            return Path(os.path.realpath(str(path)))
        else:
            return path.resolve(strict=False)

    def _normalize(self, fpath):
        return self._resolve(self._baseDir / fpath)

    def encode(self, data):
        return jsonpickle.encode(data, keys=True)

    def decode(self, data):
        return jsonpickle.decode(data, backend=WalletCompatibilityBackend(),
                                 keys=True)

    def saveWallet(self, wallet, fpath):
        """Save wallet into specified localtion.

        Returns the canonical path for the ``fpath`` where ``wallet``
        has been stored.

        Error cases:
            - ``fpath`` is not inside the keyrings base dir - ValueError raised
            - directory part of ``fpath`` exists and it's not a directory -
              NotADirectoryError raised
            - ``fpath`` exists and it's a directory - IsADirectoryError raised

        :param wallet: wallet to save
        :param fpath: wallet file path, absolute or relative to
            keyrings base dir
        """
        if not fpath:
            raise ValueError("empty path")

        _fpath = self._normalize(fpath)
        _dpath = _fpath.parent

        try:
            _dpath.relative_to(self._baseDir)
        except ValueError:
            raise ValueError(
                "path {} is not is not relative to the keyrings {}".format(
                    fpath, self._baseDir))

        self._createDirIfNotExists(_dpath)

        # ensure permissions from the bottom of the directory hierarchy
        while _dpath != self._baseDir:
            self._ensurePermissions(_dpath, self.dmode)
            _dpath = _dpath.parent

        with _fpath.open("w") as wf:
            self._ensurePermissions(_fpath, self.fmode)
            encodedWallet = self.encode(wallet)
            wf.write(encodedWallet)
            logger.debug("stored wallet '{}' in {}".format(
                wallet.name, _fpath))

        return str(_fpath)

    def loadWallet(self, fpath):
        """Load wallet from specified localtion.

        Returns loaded wallet.

        Error cases:
            - ``fpath`` is not inside the keyrings base dir - ValueError raised
            - ``fpath`` exists and it's a directory - IsADirectoryError raised

        :param fpath: wallet file path, absolute or relative to
            keyrings base dir
        """
        if not fpath:
            raise ValueError("empty path")

        _fpath = self._normalize(fpath)
        _dpath = _fpath.parent

        try:
            _dpath.relative_to(self._baseDir)
        except ValueError:
            raise ValueError(
                "path {} is not is not relative to the wallets {}".format(
                    fpath, self._baseDir))

        with _fpath.open() as wf:
            wallet = self.decode(wf.read())

        return wallet


WALLET_RAW_MIGRATORS = []


class WalletCompatibilityBackend(JSONBackend):
    """
    Jsonpickle backend providing conversion of raw representations
    (nested dictionaries/lists structure) of wallets from previous versions
    to the current version.
    """

    def decode(self, string):
        raw = super().decode(string)
        # Note that backend.decode may be called not only for the whole object
        # representation but also for representations of non-string keys of
        # dictionaries.
        for migrator in WALLET_RAW_MIGRATORS:
            migrator(raw)
        return raw
