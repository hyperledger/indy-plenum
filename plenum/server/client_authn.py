"""
Clients are authenticated with a digital signature.
"""
from base64 import b64decode
from typing import Dict
from typing import Mapping

from raet.nacling import Verifier

from plenum.common.exceptions import InvalidSignature, EmptySignature, MissingSignature, EmptyIdentifier, \
    MissingIdentifier, InvalidIdentifier, CouldNotAuthenticate, SigningException
from plenum.common.request_types import f
from plenum.common.signing import serializeForSig


class ClientAuthNr:
    """
    Interface for client authenticators.
    """
    def authenticate(self, msg, identifier, signature):
        raise NotImplementedError()


class SimpleAuthNr(ClientAuthNr):
    """
    Simple client authenticator. Should be replaced with a more robust and
    secure system.
    """
    def __init__(self):
        # key: some identifier, value: verification key
        self.clients = {}  # type: Dict[str, str]

    def authenticate(self,
                     msg: Mapping,
                     identifier: str=None,
                     signature: str=None) -> bool:
        """
        Authenticate the client's message with the signature provided.

        :param identifier: some unique identifier; if None, then try to use
        msg['clientId'] as identifier
        :param signature: a utf-8 and base64 encoded signature
        :param msg: the message to authenticate
        :return: the identifier; an exception of type SigningException is
            raised if the signature is not valid
        """
        try:
            if not signature:
                try:
                    signature = msg["signature"]
                    if not signature:
                        raise EmptySignature
                except KeyError:
                    raise MissingSignature
            if not identifier:
                try:
                    identifier = msg[f.CLIENT_ID.nm]
                    if not identifier:
                        raise EmptyIdentifier
                except KeyError:
                    raise MissingIdentifier
            b64sig = signature.encode('utf-8')
            sig = b64decode(b64sig)
            ser = serializeForSig(msg)
            try:
                verkey = self.clients[identifier]
            except KeyError:
                raise InvalidIdentifier
            vr = Verifier(verkey)
            isVerified = vr.verify(sig, ser)
            if not isVerified:
                raise InvalidSignature
        except SigningException:
            raise
        except Exception as ex:
            raise CouldNotAuthenticate from ex
        return identifier

    def addClient(self, identifier, verkey):
        """
        Adding a client should be an auditable and authenticated action.
        Robust implementations of ClientAuthNr would authenticate this
        operation.

        :param identifier: an identifier that directly or indirectly identifies a client
        :param verkey: the public key used to verify a signature
        :return: None
        """
        if identifier in self.clients:
            raise RuntimeError("client already added")
        self.clients[identifier] = verkey
