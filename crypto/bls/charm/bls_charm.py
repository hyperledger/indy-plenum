from copy import copy
from functools import reduce
from typing import Any, Sequence

from charm.core.engine.util import objectToBytes
from charm.core.math.pairing import G2, G1, pair
from charm.toolbox.pairinggroup import PairingGroup
from crypto.bls.bls import BlsSignature


class BlsSignatureCharm(BlsSignature):
    @staticmethod
    def default_params():
        group_name = 'MNT224'
        group = PairingGroup(group_name)
        g = group.random(G2)
        return group_name, g

    def __init__(self, group_name=None, g=None):
        if not group_name or not g:
            group_name, g = self.default_params()
        self.group = PairingGroup(group_name)
        self.g = g

    def generate_keys(self) -> (Any, Any):
        self.sk = self.group.random()
        self.pk = self.g ** self.sk
        return (self.sk, self.pk)

    def sign(self, message) -> Any:
        assert self.sk
        assert self.pk
        M = self._msgForSign(message, self.pk)
        return self.group.hash(M, G1) ** self.sk

    def create_multi_sig(self, signatures: Sequence) -> Any:
        return reduce(lambda x, y: x * y, signatures)

    def verify_sig(self, signature, message, pk) -> bool:
        assert self.g
        h = self._h(message, pk)
        return pair(signature, self.g) == pair(h, pk)

    def verify_multi_sig(self, signature, messages_with_pk: Sequence) -> bool:
        assert self.g
        multi_sig_e_list = []
        for msg, pki in messages_with_pk:
            h = self._h(msg, pki)
            multi_sig_e_list.append(pair(h, pki))

        multi_sig_e = reduce(lambda x, y: x * y, multi_sig_e_list)
        return pair(signature, self.g) == multi_sig_e

    def _h(self, msg, pk):
        M = self._msgForSign(msg, pk)
        return self.group.hash(M, G1)

    def _msgForSign(self, message, pk):
        msg = self._dump(message)
        msg_pk = self.group.serialize(pk)
        return msg + msg_pk

    def _dump(self, obj):
        return objectToBytes(obj, self.group)
