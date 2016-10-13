import base58
from binascii import hexlify
from typing import Dict

from libnacl import randombytes
from raet.nacling import SigningKey
from raet.nacling import Signer as NaclSigner

from plenum.common.signer import Signer
from plenum.common.signing import serializeForSig

from plenum.common.util import hexToFriendly, rawToFriendly


class DidSigner(Signer):
    """
    A simple implementation of Signer for DIDs (Distributed Identifiers).

    This signer creates a public key and a private key using the seed value
    provided in the constructor. It internally uses the NaclSigner to generate
    the signature and keys.
    """

    # TODO: Do we need both alias and identifier?
    def __init__(self, identifier=None, seed=None, alias=None):
        """
        Initialize the signer with an identifier and a seed.

        :param identifier: some identifier that directly or indirectly
        references this client
        :param seed: the seed used to generate a signing key.
        """

        # should be stored securely/privately
        self.seed = seed if seed else randombytes(32)

        # generates key pair based on seed
        self.sk = SigningKey(seed=self.seed)

        # helper for signing
        self.naclSigner = NaclSigner(self.sk)

        # this is the public key used to verify signatures (securely shared
        # before-hand with recipient)
        self.verkey = self.naclSigner.verhex

        self.verstr = rawToFriendly(self.naclSigner.verraw)

        self._identifier = identifier or self.verstr

        self._alias = alias

    @property
    def alias(self) -> str:
        return self._alias

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def seedHex(self) -> bytes:
        return hexlify(self.seed)

    def sign(self, msg: Dict) -> Dict:
        """
        Return a signature for the given message.
        """
        ser = serializeForSig(msg)
        bsig = self.naclSigner.signature(ser)
        sig = base58.b58encode(bsig)
        return sig
