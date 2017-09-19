from typing import Sequence

from crypto.bls.bls_crypto import BlsCrypto, GroupParams, BlsGroupParamsLoader
from indy_crypto.bls import BlsEntity, Generator, VerKey, SignKey, Bls, Signature, MultiSignature


class BlsGroupParamsLoaderIndyCrypto(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'generator'
        g = Generator.new()
        return GroupParams(group_name, g)


class BlsCryptoIndyCrypto(BlsCrypto):
    def __init__(self, sk: str, pk: str, params: GroupParams):
        super().__init__(sk, pk, params)
        self._generator = params.g
        self._sk_bls = BlsCryptoIndyCrypto._bls_from_str(sk, SignKey)
        self._pk_bls = BlsCryptoIndyCrypto._bls_from_str(pk, VerKey)

    @staticmethod
    def _bls_to_str(v: BlsEntity) -> str:
        return v.as_bytes().hex()

    @staticmethod
    def _bls_from_str(v: str, cls) -> BlsEntity:
        bts = bytes.fromhex(v)
        return BlsEntity.from_bytes(cls, bts)

    @staticmethod
    def _msg_to_str(msg: bytes) -> str:
        return msg.hex()

    @staticmethod
    def _msg_from_str(msg: str) -> bytes:
        return bytes.fromhex(msg)

    @staticmethod
    def _msg_to_bls_bytes(msg: str) -> bytes:
        return msg.encode()

    @staticmethod
    def generate_keys(params: GroupParams, seed=None) -> (str, str):
        sk = SignKey.new(BlsCryptoIndyCrypto._prepare_seed(seed))
        vk = VerKey.new(params.g, sk)
        sk_str = BlsCryptoIndyCrypto._bls_to_str(sk)
        vk_str = BlsCryptoIndyCrypto._bls_to_str(vk)
        return sk_str, vk_str

    @staticmethod
    def _prepare_seed(seed):
        if isinstance(seed, str):
            return seed.encode()
        if isinstance(seed, bytes):
            return seed
        if isinstance(seed, bytearray):
            return seed
        return None

    def sign(self, message: str) -> str:
        bts = BlsCryptoIndyCrypto._msg_to_bls_bytes(message)
        sign = Bls.sign(bts, self._sk_bls)
        return BlsCryptoIndyCrypto._bls_to_str(sign)

    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        sigs = [BlsCryptoIndyCrypto._bls_to_str(s) for s in signatures]
        bts = MultiSignature.new(sigs)
        return BlsCryptoIndyCrypto._bls_to_str(bts)

    def verify_sig(self, signature: str, message: str, pk: str) -> bool:
        return Bls.verify(BlsCryptoIndyCrypto._bls_from_str(signature, Signature),
                          BlsCryptoIndyCrypto._msg_to_bls_bytes(message),
                          BlsCryptoIndyCrypto._bls_from_str(pk, VerKey),
                          self._generator)

    def verify_multi_sig(self, signature: str, message: str, pks: Sequence[str]) -> bool:
        epks = [BlsCryptoIndyCrypto._bls_to_str(s) for s in pks]
        return Bls.verify_multi_sig(BlsCryptoIndyCrypto._bls_from_str(signature, MultiSignature),
                                    BlsCryptoIndyCrypto._msg_to_bls_bytes(message),
                                    epks,
                                    self._generator)
