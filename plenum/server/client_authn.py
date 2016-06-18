"""
Clients are authenticated with a digital signature.
"""
from abc import abstractmethod
from base64 import b64decode
from typing import Dict

from raet.nacling import Verifier

from plenum.common.exceptions import InvalidSignature, EmptySignature, \
    MissingSignature, EmptyIdentifier, \
    MissingIdentifier, InvalidIdentifier, CouldNotAuthenticate, SigningException
from plenum.common.signing import serializeForSig
from plenum.common.types import f


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
        :param signature: a utf-8 and base64 encoded signature
        :param msg: the message to authenticate
        :return: the identifier; an exception of type SigningException is
            raised if the signature is not valid
        """

    @abstractmethod
    def addClient(self, identifier, verkey, pubkey=None, role=None):
        """
        Adding a client should be an auditable and authenticated action.
        Robust implementations of ClientAuthNr would authenticate this
        operation.

        :param identifier: an identifier that directly or indirectly identifies
            a client
        :param verkey: the public key used to verify a signature
        :return: None
        """
        pass

    @abstractmethod
    def getVerkey(self, identifier):
        """
        Get the verification key for a client based on the client's identifier

        :param identifier: client's identifier
        :return: the verification key
        """
        pass


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
                        raise EmptySignature(identifier, msg.get(f.REQ_ID.nm))
                except KeyError:
                    raise MissingSignature
            if not identifier:
                try:
                    identifier = msg[f.IDENTIFIER.nm]
                    if not identifier:
                        raise EmptyIdentifier
                except KeyError:
                    raise MissingIdentifier
            b64sig = signature.encode('utf-8')
            sig = b64decode(b64sig)
            ser = self.serializeForSig(msg)
            try:
                verkey = self.getVerkey(identifier)
            except KeyError:
                # TODO: Should probably be called UnknownIdentifier
                raise InvalidIdentifier(identifier, msg.get(f.REQ_ID.nm))
            vr = Verifier(verkey)
            isVerified = vr.verify(sig, ser)
            if not isVerified:
                raise InvalidSignature
        except SigningException as e:
            raise e
        except Exception as ex:
            raise CouldNotAuthenticate from ex
        return identifier

    @abstractmethod
    def addClient(self, identifier, verkey, pubkey=None, role=None):
        pass

    @abstractmethod
    def getVerkey(self, identifier):
        pass

    def serializeForSig(self, msg):
        return serializeForSig(msg)


class SimpleAuthNr(NaclAuthNr):
    """
    Simple client authenticator. Should be replaced with a more robust and
    secure system.
    """

    def __init__(self):
        # key: some identifier, value: verification key
        self.clients = {}  # type: Dict[str, Dict]

    def addClient(self, identifier, verkey, pubkey=None, role=None):
        if identifier in self.clients:
            raise RuntimeError("client already added")
        self.clients[identifier] = {
            "verkey": verkey,
            "pubkey": pubkey,
            "role": role
        }

    def getVerkey(self, identifier):
        return self.clients[identifier]["verkey"]
