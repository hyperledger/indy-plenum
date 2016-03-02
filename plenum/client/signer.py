from typing import Mapping

from libnacl import randombytes
from libnacl.encode import base64_encode
from raet.nacling import Signer as NaclSigner
from raet.nacling import SigningKey

from plenum.common.signing import serializeForSig


class Signer:
    """
    Interface that defines a sign method.
    """
    def sign(self, msg):
        raise NotImplementedError()


class SimpleSigner(Signer):
    """
    A simple implementation of Signer.

    This signer creates a public key and a private key using the seed value provided in the constructor.
    It internally uses the NaclSigner to generate the signature and keys.
    """
    def __init__(self, identifier, seed=None):

        """
        Initialize the signer with an identifier and a seed.

        :param identifier: some identifier that directly or indirectly references this client
        :param seed: the seed used to generate a signing key.
        """

        self.identifier = identifier

        # should be stored securely/privately
        self.seed = seed if seed else randombytes(32)

        # generates key pair based on seed
        self.sk = SigningKey(seed=self.seed)

        # helper for signing
        self.naclSigner = NaclSigner(self.sk)

        # this is the public key used to verify signatures (securely shared
        # before-hand with recipient)
        self.verkey = self.naclSigner.verhex

        self.verstr = base64_encode(self.naclSigner.keyraw).decode('utf-8')

    def sign(self, msg: Mapping) -> Mapping:
        """
        Return a signature for the given message.
        """
        ser = serializeForSig(msg)
        bsig = self.naclSigner.signature(ser)
        b64sig = base64_encode(bsig)
        sig = b64sig.decode('utf-8')
        return sig
