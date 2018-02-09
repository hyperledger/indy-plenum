"""
Clients are authenticated with a digital signature.
"""
from abc import abstractmethod
from typing import Dict

import base58
from common.serializers.serialization import serialize_msg_for_signing
from plenum.common.constants import VERKEY, ROLE, GET_TXN
from plenum.common.exceptions import EmptySignature, \
    MissingSignature, EmptyIdentifier, \
    MissingIdentifier, CouldNotAuthenticate, \
    InvalidSignatureFormat, UnknownIdentifier, \
    InsufficientSignatures, InsufficientCorrectSignatures
from plenum.common.types import f
from plenum.common.verifier import DidVerifier, Verifier
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.pool_req_handler import PoolRequestHandler
from stp_core.common.log import getlogger

logger = getlogger()


class ClientAuthNr:
    """
    Interface for client authenticators.
    """

    @abstractmethod
    def authenticate(self,
                     msg: Dict,
                     identifier: str = None,
                     signature: str = None) -> str:
        """
        Authenticate the client's message with the signature provided.

        :param identifier: some unique identifier; if None, then try to use
        msg['identifier'] as identifier
        :param signature: a utf-8 and base58 encoded signature
        :param msg: the message to authenticate
        :return: the identifier; an exception of type SigningException is
            raised if the signature is not valid
        """

    @abstractmethod
    def authenticate_multi(self, msg: Dict, signatures: Dict[str, str],
                           threshold: int = None):
        """
        :param msg:
        :param signatures: A mapping from identifiers to signatures.
        :param threshold: The number of successful signature verification
        required. By default all signatures are required to be verified.
        :return: returns the identifiers whose signature was matched and
        correct; a SigningException is raised if threshold was not met
        """

    @abstractmethod
    def addIdr(self, identifier, verkey, role=None):
        """
        Adding an identifier should be an auditable and authenticated action.
        Robust implementations of ClientAuthNr would authenticate this
        operation.

        :param identifier: an identifier that directly or indirectly identifies
            a client
        :param verkey: the public key used to verify a signature
        :return: None
        """

    @abstractmethod
    def getVerkey(self, identifier):
        """
        Get the verification key for a client based on the client's identifier

        :param identifier: client's identifier
        :return: the verification key
        """


class NaclAuthNr(ClientAuthNr):

    def authenticate_multi(self, msg: Dict, signatures: Dict[str, str],
                           threshold: int=None, verifier: Verifier=DidVerifier):
        num_sigs = len(signatures)
        if threshold is not None:
            if num_sigs < threshold:
                raise InsufficientSignatures(num_sigs, threshold)
        else:
            threshold = num_sigs
        correct_sigs_from = []
        for idr, sig in signatures.items():
            try:
                sig = base58.b58decode(sig)
            except Exception as ex:
                raise InvalidSignatureFormat from ex

            ser = self.serializeForSig(msg, identifier=idr)
            verkey = self.getVerkey(idr)

            if verkey is None:
                raise CouldNotAuthenticate(
                    'Can not find verkey for {}'.format(idr))

            vr = verifier(verkey, identifier=idr)
            if vr.verify(sig, ser):
                correct_sigs_from.append(idr)
                if len(correct_sigs_from) == threshold:
                    break
        else:
            raise InsufficientCorrectSignatures(len(correct_sigs_from),
                                                threshold)
        return correct_sigs_from

    @abstractmethod
    def addIdr(self, identifier, verkey, role=None):
        pass

    @abstractmethod
    def getVerkey(self, identifier):
        pass

    def serializeForSig(self, msg, identifier=None, topLevelKeysToIgnore=None):
        return serialize_msg_for_signing(
            msg, topLevelKeysToIgnore=topLevelKeysToIgnore)


class SimpleAuthNr(NaclAuthNr):
    """
    Simple client authenticator. Should be replaced with a more robust and
    secure system.
    """

    def __init__(self, state=None):
        # key: some identifier, value: verification key
        self.clients = {}  # type: Dict[str, Dict]
        self.state = state

    def addIdr(self, identifier, verkey, role=None):
        if identifier in self.clients:
            # raise RuntimeError("client already added")
            logger.debug("client already added")
        self.clients[identifier] = {
            VERKEY: verkey,
            ROLE: role
        }

    def getVerkey(self, identifier):
        nym = self.clients.get(identifier)
        if not nym:
            # Querying uncommitted identities since a batch might contain
            # both identity creation request and a request by that newly
            # created identity, also its possible to have multiple uncommitted
            # batches in progress and identity creation request might
            # still be in an earlier uncommited batch
            nym = DomainRequestHandler.getNymDetails(
                self.state, identifier, isCommitted=False)
            if not nym:
                raise UnknownIdentifier(identifier)
        return nym.get(VERKEY)

    def authenticate(self,
                     msg: Dict,
                     identifier: str = None,
                     signature: str = None):
        signatures = {identifier: signature}
        return self.authenticate_multi(msg,
                                       signatures=signatures)


class CoreAuthMixin:
    # TODO: This should know a list of valid fields rather than excluding
    # hardcoded fields
    excluded_from_signing = {f.SIG.nm, f.SIGS.nm}
    write_types = PoolRequestHandler.write_types.union(
        DomainRequestHandler.write_types
    )
    query_types = {GET_TXN, }.union(
        PoolRequestHandler.query_types
    ).union(
        DomainRequestHandler.query_types
    )

    @classmethod
    def is_query(cls, typ):
        return typ in cls.query_types

    @classmethod
    def is_write(cls, typ):
        return typ in cls.write_types

    @staticmethod
    def _extract_signature(msg):
        if f.SIG.nm not in msg:
            raise MissingSignature
        if not msg[f.SIG.nm]:
            raise EmptySignature
        return msg[f.SIG.nm]

    @staticmethod
    def _extract_identifier(msg):
        if f.IDENTIFIER.nm not in msg:
            raise MissingIdentifier
        if not msg[f.IDENTIFIER.nm]:
            raise EmptyIdentifier
        return msg[f.IDENTIFIER.nm]

    def authenticate(self, req_data, identifier: str=None,
                     signature: str=None, verifier: Verifier=DidVerifier):
        """
        Prepares the data to be serialised for signing and then verifies the
        signature
        :param req_data:
        :param identifier:
        :param signature:
        :param verifier:
        :return:
        """
        to_serialize = {k: v for k, v in req_data.items()
                        if k not in self.excluded_from_signing}
        if req_data.get(f.SIG.nm) is None and \
                req_data.get(f.SIGS.nm) is None and \
                signature is None:
            raise MissingSignature
        if req_data.get(f.IDENTIFIER.nm) and (req_data.get(f.SIG.nm) or
                                              signature):
            try:
                # if not identifier:
                identifier = identifier or self._extract_identifier(req_data)

                # if not signature:
                signature = signature or self._extract_signature(req_data)

                signatures = {identifier: signature}
            except Exception as ex:
                if ex in (MissingSignature, EmptySignature, MissingIdentifier,
                          EmptyIdentifier):
                    ex = ex(req_data.get(f.IDENTIFIER.nm), req_data.get(f.SIG.nm))
                raise ex
        else:
            signatures = req_data[f.SIGS.nm]
        return self.authenticate_multi(to_serialize,
                                       signatures=signatures, verifier=verifier)

    def serializeForSig(self, msg, identifier=None, topLevelKeysToIgnore=None):
        if not msg.get(f.IDENTIFIER.nm):
            msg = {**msg, f.IDENTIFIER.nm: identifier}
        return serialize_msg_for_signing(
            msg, topLevelKeysToIgnore=topLevelKeysToIgnore)


class CoreAuthNr(CoreAuthMixin, SimpleAuthNr):
    def __init__(self, state=None):
        SimpleAuthNr.__init__(self, state)
        CoreAuthMixin.__init__(self)
