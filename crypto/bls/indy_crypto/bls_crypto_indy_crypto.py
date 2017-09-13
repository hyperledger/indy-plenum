from typing import Sequence

from crypto.bls.bls_crypto import BlsCrypto, GroupParams, BlsGroupParamsLoader
from indy_crypto import bls as indy_bls


class BlsGroupParamsLoaderIndyCrypto(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'generator'
        g = indy_bls.create_generator()
        return GroupParams(group_name, g)


class BlsCryptoIndyCrypto(BlsCrypto):
    def __init__(self, sk: str, pk: str, params: GroupParams):
        super().__init__(sk, pk, params)
        self._generator = params.g
        self._sk_ec = bytes.fromhex(sk)
        self._pk_ec = bytes.fromhex(pk)

    @staticmethod
    def generate_keys(params: GroupParams, seed=None) -> (str, str):
        sk, vk = indy_bls.generate_keys(params.g, BlsCryptoIndyCrypto._prepare_seed(seed))
        sk_str = sk.hex()
        vk_str = vk.hex()

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
        msg = message.encode()
        sign = indy_bls.sign(msg, self._sk_ec)
        return sign.hex()

    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        sigs = [bytes.fromhex(s) for s in signatures]
        msig = indy_bls.create_multi_signature(sigs)
        return msig.hex()

    def verify_sig(self, signature: str, message: str, pk: str) -> bool:
        return indy_bls.verify(bytes.fromhex(signature), message.encode(), bytes.fromhex(pk), self._generator)

    def verify_multi_sig(self, signature: str, message: str, pks: Sequence[str]) -> bool:
        epks = [bytes.fromhex(s) for s in pks]
        return indy_bls.verify_multi_sig(bytes.fromhex(signature), message.encode(), epks, self._generator)
