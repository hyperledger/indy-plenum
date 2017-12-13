# from functools import reduce
# from typing import Any, Sequence
#
# from charm.core.engine.util import objectToBytes, bytesToObject
# from charm.core.math.pairing import G1, pair
# from charm.toolbox.pairinggroup import PairingGroup
# from crypto.bls.bls_crypto import BlsCrypto, GroupParams, BlsGroupParamsLoader
#
#
# class BlsGroupParamsLoaderCharmHardcoded(BlsGroupParamsLoader):
#     def load_group_params(self) -> GroupParams:
#         group_name = 'MNT224'
#         g_ser = b'2:a+JE7oFvoxCguOi6ok/wAvRQcXwroCEh94UE61ptvdgHd4UeU12l0qdhA9FpzzOKE+/Zf6cW0BsQ6pl3Elh24StUosxLUn/2CIEeFFMpgxeFHQD4AQ=='
#         group = PairingGroup(group_name)
#         g = group.deserialize(g_ser)
#         return GroupParams(group_name, g)
#
#
# class BlsSerializerCharm:
#     def __init__(self, params: GroupParams):
#         self.group = BlsCryptoCharm._create_group(params)
#
#     def serialize_to_bytes(self, obj: Any) -> bytes:
#         return objectToBytes(obj, self.group)
#
#     def deserialize_from_bytes(self, obj: bytes) -> Any:
#         return bytesToObject(obj, self.group)
#
#     def serialize_to_str(self, obj: Any) -> str:
#         return self.serialize_to_bytes(obj).decode()
#
#     def deserialize_from_str(self, obj: str) -> Any:
#         return self.deserialize_from_bytes(obj.encode())
#
#
# class BlsCryptoCharm(BlsCrypto):
#     def __init__(self, sk: str, pk: str, params: GroupParams):
#         super().__init__(sk, pk, params)
#         self.group = self._create_group(params)
#         self.g = params.g
#         self._serializer = BlsSerializerCharm(params)
#         self._sk_ec = self._serializer.deserialize_from_str(sk)
#         self._pk_ec = self._serializer.deserialize_from_str(pk)
#
#     @staticmethod
#     def _create_group(params: GroupParams) -> PairingGroup:
#         return PairingGroup(params.group_name)
#
#     @staticmethod
#     def generate_keys(params: GroupParams, seed=None) -> (str, str):
#         seed = BlsCryptoCharm._prepare_seed(seed)
#         group = BlsCryptoCharm._create_group(params)
#         sk = group.random(seed=seed)
#         pk = params.g ** sk
#
#         serializer = BlsSerializerCharm(params)
#         sk_str = serializer.serialize_to_str(sk)
#         pk_str = serializer.serialize_to_str(pk)
#
#         return sk_str, pk_str
#
#     @staticmethod
#     def _prepare_seed(seed):
#         if not seed:
#             return None
#         if isinstance(seed, int):
#             return seed
#         if isinstance(seed, str):
#             seed = seed.encode()
#         if not isinstance(seed, (bytes, bytearray)):
#             raise RuntimeError('Unknown seed type for BLS keys. Must be either int, str or bytes')
#         return int.from_bytes(seed[:4], byteorder='little')
#
#     def sign(self, message: str) -> str:
#         M = self._msgForSign(message, self.pk)
#         signature = self.group.hash(M, G1) ** self._sk_ec
#         return self._serializer.serialize_to_str(signature)
#
#     def create_multi_sig(self, signatures: Sequence[str]) -> str:
#         signatures = [self._serializer.deserialize_from_str(signature) for signature in signatures]
#         multi_sig = reduce(lambda x, y: x * y, signatures)
#         return self._serializer.serialize_to_str(multi_sig)
#
#     def verify_sig(self, signature: str, message: str, pk: str) -> bool:
#         h = self._h(message, pk)
#         signature = self._serializer.deserialize_from_str(signature)
#         pk = self._serializer.deserialize_from_str(pk)
#         return pair(signature, self.g) == pair(h, pk)
#
#     def verify_multi_sig(self, signature: str, message: str, pks: Sequence[str]) -> bool:
#         multi_sig_e_list = []
#         for pk in pks:
#             h = self._h(message, pk)
#             pk = self._serializer.deserialize_from_str(pk)
#             multi_sig_e_list.append(pair(h, pk))
#
#         multi_sig_e = reduce(lambda x, y: x * y, multi_sig_e_list)
#         signature = self._serializer.deserialize_from_str(signature)
#         return pair(signature, self.g) == multi_sig_e
#
#     def _h(self, msg: str, pk: str):
#         M = self._msgForSign(msg, pk)
#         return self.group.hash(M, G1)
#
#     def _msgForSign(self, message: str, msg_pk: str):
#         return message + msg_pk
