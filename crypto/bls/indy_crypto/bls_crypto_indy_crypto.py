from typing import Sequence

import base58
from crypto.bls.bls_crypto import BlsCrypto, GroupParams, BlsGroupParamsLoader
from indy_crypto.bls import BlsEntity, Generator, VerKey, SignKey, Bls, Signature, MultiSignature


class BlsGroupParamsLoaderIndyCrypto(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'generator'
        g = "3LHpUjiyFC2q2hD7MnwwNmVXiuaFbQx2XkAFJWzswCjgN1utjsCeLzHsKk1nJvFEaS4fcrUmVAkdhtPCYbrVyATZcmzwJReTcJqwqBCPTmTQ9uWPwz6rEncKb2pYYYFcdHa8N17HzVyTqKfgPi4X9pMetfT3A5xCHq54R2pDNYWVLDX"
        return GroupParams(group_name, g)


class BlsCryptoIndyCrypto(BlsCrypto):
    SEED_LEN = 48

    def __init__(self, sk: str, pk: str, params: GroupParams):
        super().__init__(sk, pk, params)
        self._generator = BlsCryptoIndyCrypto._bls_from_str(params.g, Generator)
        self._sk_bls = BlsCryptoIndyCrypto._bls_from_str(sk, SignKey)
        self._pk_bls = BlsCryptoIndyCrypto._bls_from_str(pk, VerKey)

    @staticmethod
    def _bls_to_str(v: BlsEntity) -> str:
        return base58.b58encode(v.as_bytes())

    @staticmethod
    def _bls_from_str(v: str, cls) -> BlsEntity:
        bts = base58.b58decode(v)
        return cls.from_bytes(bts)

    @staticmethod
    def _msg_to_bls_bytes(msg: str) -> bytes:
        return msg.encode()

    @staticmethod
    def generate_keys(params: GroupParams, seed=None) -> (str, str):
        seed = BlsCryptoIndyCrypto._prepare_seed(seed)
        gen = BlsCryptoIndyCrypto._bls_from_str(params.g, Generator)
        sk = SignKey.new(seed)
        vk = VerKey.new(gen, sk)
        sk_str = BlsCryptoIndyCrypto._bls_to_str(sk)
        vk_str = BlsCryptoIndyCrypto._bls_to_str(vk)
        return sk_str, vk_str

    @staticmethod
    def _prepare_seed(seed):
        seed_bytes = None
        if isinstance(seed, str):
            seed_bytes = seed.encode()
        if isinstance(seed, (bytes, bytearray)):
            seed_bytes = seed

        # TODO: FIXME: indy-crupto supports 48-bit seeds only
        if seed_bytes:
            if len(seed_bytes) < BlsCryptoIndyCrypto.SEED_LEN:
                seed_bytes += b'0' * (BlsCryptoIndyCrypto.SEED_LEN - len(seed_bytes))
            assert (len(seed_bytes) == BlsCryptoIndyCrypto.SEED_LEN)

        return seed_bytes

    def sign(self, message: str) -> str:
        bts = BlsCryptoIndyCrypto._msg_to_bls_bytes(message)
        sign = Bls.sign(bts, self._sk_bls)
        return BlsCryptoIndyCrypto._bls_to_str(sign)

    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        sigs = [BlsCryptoIndyCrypto._bls_from_str(s, Signature) for s in signatures]
        bts = MultiSignature.new(sigs)
        return BlsCryptoIndyCrypto._bls_to_str(bts)

    def verify_sig(self, signature: str, message: str, pk: str) -> bool:
        return Bls.verify(BlsCryptoIndyCrypto._bls_from_str(signature, Signature),
                          BlsCryptoIndyCrypto._msg_to_bls_bytes(message),
                          BlsCryptoIndyCrypto._bls_from_str(pk, VerKey),
                          self._generator)

    def verify_multi_sig(self, signature: str, message: str, pks: Sequence[str]) -> bool:
        epks = [BlsCryptoIndyCrypto._bls_from_str(p, VerKey) for p in pks]
        return Bls.verify_multi_sig(BlsCryptoIndyCrypto._bls_from_str(signature, MultiSignature),
                                    BlsCryptoIndyCrypto._msg_to_bls_bytes(message),
                                    epks,
                                    self._generator)
