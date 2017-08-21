"""
Clients are authenticated with a digital signature.
"""
from abc import abstractmethod
from typing import Dict

import base58
from common.serializers.serialization import serialize_msg_for_signing
from plenum.common.constants import VERKEY, ROLE
from plenum.common.exceptions import InvalidSignature, EmptySignature, \
    MissingSignature, EmptyIdentifier, \
    MissingIdentifier, CouldNotAuthenticate, \
    SigningException, InvalidSignatureFormat, UnknownIdentifier
from plenum.common.types import f
from plenum.common.verifier import DidVerifier
from plenum.server.domain_req_handler import DomainRequestHandler
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
    def authenticate(self,
                     msg: Dict,
                     identifier: str = None,
                     signature: str = None) -> str:
        try:
            if not signature:
                try:
                    signature = msg[f.SIG.nm]
                    if not signature:
                        raise EmptySignature(msg.get(f.IDENTIFIER.nm),
                                             msg.get(f.REQ_ID.nm))
                except KeyError:
                    raise MissingSignature(msg.get(f.IDENTIFIER.nm),
                                           msg.get(f.REQ_ID.nm))
            if not identifier:
                try:
                    identifier = msg[f.IDENTIFIER.nm]
                    if not identifier:
                        raise EmptyIdentifier(None, msg.get(f.REQ_ID.nm))
                except KeyError:
                    raise MissingIdentifier(identifier, msg.get(f.REQ_ID.nm))
            try:
                sig = base58.b58decode(signature)
            except Exception as ex:
                raise InvalidSignatureFormat from ex
            ser = self.serializeForSig(msg, topLevelKeysToIgnore=[f.SIG.nm])
            verkey = self.getVerkey(identifier)

            if verkey is None:
                raise CouldNotAuthenticate(
                    'Can not find verkey for DID {}'.format(identifier))

            vr = DidVerifier(verkey, identifier=identifier)
            isVerified = vr.verify(sig, ser)
            if not isVerified:
                raise InvalidSignature
        except SigningException as e:
            raise e
        except Exception as ex:
            raise CouldNotAuthenticate from ex
        return identifier

    @abstractmethod
    def addIdr(self, identifier, verkey, role=None):
        pass

    @abstractmethod
    def getVerkey(self, identifier):
        pass

    def serializeForSig(self, msg, topLevelKeysToIgnore=None):
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
