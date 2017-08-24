from functools import reduce
from typing import Any, Sequence

from charm.core.engine.util import objectToBytes, bytesToObject
from charm.core.math.pairing import G1, pair
from charm.toolbox.pairinggroup import PairingGroup
from crypto.bls.bls_crypto import BlsCrypto, GroupParams, BlsGroupParamsLoader, BlsSerializer


class BlsGroupParamsLoaderCharmHardcoded(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'MNT224'
        g_ser = b'2:a+JE7oFvoxCguOi6ok/wAvRQcXwroCEh94UE61ptvdgHd4UeU12l0qdhA9FpzzOKE+/Zf6cW0BsQ6pl3Elh24StUosxLUn/2CIEeFFMpgxeFHQD4AQ=='
        group = PairingGroup(group_name)
        g = group.deserialize(g_ser)
        return GroupParams(group_name, g)


class BlsSerializerCharm(BlsSerializer):
    def __init__(self, params: GroupParams):
        super().__init__(params)
        self.group = BlsCryptoCharm._create_group(params)

    def serialize(self, obj: Any) -> bytes:
        return objectToBytes(obj, self.group)

    def deserialize(self, obj: bytes) -> Any:
        return bytesToObject(obj, self.group)


class BlsCryptoCharm(BlsCrypto):
    def __init__(self, sk: Any, pk: Any, params: GroupParams, serializer: BlsSerializer):
        super().__init__(sk, pk, params, serializer)
        self.group = self._create_group(params)
        self.g = params.g

    @staticmethod
    def _create_group(params: GroupParams) -> PairingGroup:
        return PairingGroup(params.group_name)

    @staticmethod
    def generate_keys(params: GroupParams) -> (Any, Any):
        group = BlsCryptoCharm._create_group(params)
        sk = group.random()
        pk = params.g ** sk
        return sk, pk

    def sign(self, message) -> Any:
        M = self._msgForSign(message, self.pk)
        return self.group.hash(M, G1) ** self._sk

    def create_multi_sig(self, signatures: Sequence) -> Any:
        return reduce(lambda x, y: x * y, signatures)

    def verify_sig(self, signature, message, pk) -> bool:
        h = self._h(message, pk)
        return pair(signature, self.g) == pair(h, pk)

    def verify_multi_sig(self, signature, message, pks: Sequence) -> bool:
        multi_sig_e_list = []
        for pk in pks:
            h = self._h(message, pk)
            multi_sig_e_list.append(pair(h, pk))

        multi_sig_e = reduce(lambda x, y: x * y, multi_sig_e_list)
        return pair(signature, self.g) == multi_sig_e

    def _h(self, msg, pk):
        M = self._msgForSign(msg, pk)
        return self.group.hash(M, G1)

    def _msgForSign(self, message, pk):
        msg = self._serializer.serialize(message)
        msg_pk = self._serializer.serialize(pk)
        return msg + msg_pk
