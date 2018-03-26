from binascii import hexlify
from typing import Dict

import base58
from common.serializers.serialization import serialize_msg_for_signing
from libnacl import randombytes
from plenum.common.types import f
from plenum.common.util import rawToFriendly, friendlyToRaw
from stp_core.crypto.nacl_wrappers import SigningKey, Signer as NaclSigner
from stp_core.crypto.signer import Signer
from stp_core.types import Identifier


class DidIdentity:
    abbr_prfx = '~'

    def __init__(self, identifier, verkey=None, rawVerkey=None):
        self.abbreviated = None
        if (verkey is None or verkey == '') and (rawVerkey is None or rawVerkey == ''):
            if identifier:
                self._identifier = identifier
                if (verkey is None and rawVerkey is None):
                    self._verkey = None
                else:
                    self._verkey = ''
                return

        assert (verkey or rawVerkey) and not (verkey and rawVerkey)
        if identifier:
            self._identifier = identifier
            if rawVerkey:
                self._verkey = rawToFriendly(rawVerkey)
                self.abbreviated = False
            else:
                if verkey.startswith("~"):
                    self._verkey = verkey[1:]
                    self.abbreviated = True
                else:
                    self._verkey = verkey
                    self.abbreviated = False
        else:
            verraw = rawVerkey or friendlyToRaw(verkey)
            self._identifier = rawToFriendly(verraw[:16])
            self._verkey = rawToFriendly(verraw[16:])
            self.abbreviated = True

    @property
    def identifier(self) -> Identifier:
        return self._identifier

    @property
    def verkey(self) -> str:
        if self._verkey is None:
            return None

        if self.abbreviated:
            return self.abbr_prfx + self._verkey
        else:
            return self._verkey

    @property
    def full_verkey(self):
        if self.abbreviated:
            rtn = friendlyToRaw(self.identifier)
            rtn += friendlyToRaw(self.verkey[1:])
            return rawToFriendly(rtn)
        else:
            return self.verkey


class DidSigner(DidIdentity, Signer):
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

        Signer.__init__(self)
        DidIdentity.__init__(
            self, identifier, rawVerkey=self.naclSigner.verraw)

        self._alias = alias

    @property
    def alias(self) -> str:
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def seedHex(self) -> bytes:
        return hexlify(self.seed)

    def sign(self, msg: Dict) -> Dict:
        """
        Return a signature for the given message.
        """
        ser = serialize_msg_for_signing(msg, topLevelKeysToIgnore=[f.SIG.nm])
        bsig = self.naclSigner.signature(ser)
        sig = base58.b58encode(bsig)
        return sig
