from abc import abstractmethod

from base58 import b58decode, b58encode
from raet.nacling import Verifier as NaclVerifier


class Verifier:
    @abstractmethod
    def verify(self, sig, msg) -> bool:
        pass


class DidVerifier(Verifier):
    def __init__(self, verkey, identifier=None):
        self._verkey = None
        self._vr = None
        raw = b58decode(identifier)
        if len(raw) == 32 and not verkey:  # assume cryptonym
            verkey = identifier
        if verkey[0] == '~':  # abbreviated
            verkey = b58encode(b58decode(identifier) +
                               b58decode(verkey[1:]))
        self.verkey = verkey

    @property
    def verkey(self):
        return self._verkey

    @verkey.setter
    def verkey(self, value):
        self._verkey = value
        self._vr = NaclVerifier(b58decode(value))

    def verify(self, sig, msg) -> bool:
        return self._vr.verify(sig, msg)
