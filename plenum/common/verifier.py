from abc import abstractmethod
from typing import Dict

from base58 import b58decode, b58encode
from common.serializers.serialization import serialize_msg_for_signing
from plenum.common.exceptions import InvalidKey
from stp_core.crypto.nacl_wrappers import Verifier as NaclVerifier


class Verifier:
    @abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abstractmethod
    def verify(self, sig, msg) -> bool:
        pass

    def verifyMsg(self, sig, msg: Dict):
        ser = serialize_msg_for_signing(msg)
        return self.verify(sig, ser)


class DidVerifier(Verifier):
    def __init__(self, verkey, identifier=None):
        _verkey = verkey
        self._verkey = None
        self._vr = None
        if identifier:
            rawIdr = b58decode(identifier)
            if len(rawIdr) == 32 and not verkey:  # assume cryptonym
                verkey = identifier

            if not verkey:
                raise ValueError("'verkey' should be a non-empty string")
            if verkey[0] == '~':  # abbreviated
                verkey = b58encode(b58decode(identifier) +
                                   b58decode(verkey[1:])).decode("utf-8")
        try:
            self.verkey = verkey
        except Exception as ex:
            raise InvalidKey("verkey {}".format(_verkey)) from ex

    @property
    def verkey(self):
        return self._verkey

    @verkey.setter
    def verkey(self, value):
        self._verkey = value
        self._vr = NaclVerifier(b58decode(value))

    def verify(self, sig, msg) -> bool:
        return self._vr.verify(sig, msg)
