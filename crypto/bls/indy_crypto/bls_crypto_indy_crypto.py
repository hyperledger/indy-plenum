from typing import Sequence

import base58
from crypto.bls.bls_crypto import GroupParams, BlsGroupParamsLoader, BlsCryptoVerifier, BlsCryptoSigner
from indy_crypto.bls import BlsEntity, Generator, VerKey, SignKey, Bls, Signature, MultiSignature


class BlsGroupParamsLoaderIndyCrypto(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'generator'
        g = "3LHpUjiyFC2q2hD7MnwwNmVXiuaFbQx2XkAFJWzswCjgN1utjsCeLzHsKk1nJvFEaS4fcrUmVAkdhtPCYbrVyATZcmzwJReTcJqwqBCPTmTQ9uWPwz6rEncKb2pYYYFcdHa8N17HzVyTqKfgPi4X9pMetfT3A5xCHq54R2pDNYWVLDX"
        return GroupParams(group_name, g)


class IndyCryptoBlsUtils:
    SEED_LEN = 48

    @staticmethod
    def bls_to_str(v: BlsEntity) -> str:
        return base58.b58encode(v.as_bytes())

    @staticmethod
    def bls_from_str(v: str, cls) -> BlsEntity:
        bts = base58.b58decode(v)
        return cls.from_bytes(bts)

    @staticmethod
    def msg_to_bls_bytes(msg: str) -> bytes:
        return msg.encode()

    @staticmethod
    def prepare_seed(seed):
        seed_bytes = None
        if isinstance(seed, str):
            seed_bytes = seed.encode()
        if isinstance(seed, (bytes, bytearray)):
            seed_bytes = seed

        # TODO: FIXME: indy-crupto supports 48-bit seeds only
        if seed_bytes:
            if len(seed_bytes) < IndyCryptoBlsUtils.SEED_LEN:
                seed_bytes += b'0' * (IndyCryptoBlsUtils.SEED_LEN - len(seed_bytes))
            assert (len(seed_bytes) == IndyCryptoBlsUtils.SEED_LEN)

        return seed_bytes


class BlsCryptoVerifierIndyCrypto(BlsCryptoVerifier):
    def __init__(self, params: GroupParams):
        self._generator = \
            IndyCryptoBlsUtils.bls_from_str(params.g, Generator)  # type: Generator

    def verify_sig(self, signature: str, message: str, pk: str) -> bool:
        return Bls.verify(IndyCryptoBlsUtils.bls_from_str(signature, Signature),
                          IndyCryptoBlsUtils.msg_to_bls_bytes(message),
                          IndyCryptoBlsUtils.bls_from_str(pk, VerKey),
                          self._generator)

    def verify_multi_sig(self, signature: str, message: str, pks: Sequence[str]) -> bool:
        epks = [IndyCryptoBlsUtils.bls_from_str(p, VerKey) for p in pks]
        multi_signature = \
            IndyCryptoBlsUtils.bls_from_str(signature, MultiSignature)  # type: MultiSignature
        message_bytes = IndyCryptoBlsUtils.msg_to_bls_bytes(message)
        return Bls.verify_multi_sig(multi_sig=multi_signature,
                                    message=message_bytes,
                                    ver_keys=epks,
                                    gen=self._generator)

    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        sigs = [IndyCryptoBlsUtils.bls_from_str(s, Signature) for s in signatures]
        bts = MultiSignature.new(sigs)
        return IndyCryptoBlsUtils.bls_to_str(bts)


class BlsCryptoSignerIndyCrypto(BlsCryptoSigner):
    def __init__(self, sk: str, pk: str, params: GroupParams):
        super().__init__(sk, pk, params)
        self._sk_bls = IndyCryptoBlsUtils.bls_from_str(sk, SignKey)
        self._pk_bls = IndyCryptoBlsUtils.bls_from_str(pk, VerKey)
        self._generator = \
            IndyCryptoBlsUtils.bls_from_str(params.g, Generator)  # type: Generator

    @staticmethod
    def generate_keys(params: GroupParams, seed=None) -> (str, str):
        seed = IndyCryptoBlsUtils.prepare_seed(seed)
        gen = IndyCryptoBlsUtils.bls_from_str(params.g, Generator)
        sk = SignKey.new(seed)
        vk = VerKey.new(gen, sk)
        sk_str = IndyCryptoBlsUtils.bls_to_str(sk)
        vk_str = IndyCryptoBlsUtils.bls_to_str(vk)
        return sk_str, vk_str

    def sign(self, message: str) -> str:
        bts = IndyCryptoBlsUtils.msg_to_bls_bytes(message)
        sign = Bls.sign(bts, self._sk_bls)
        return IndyCryptoBlsUtils.bls_to_str(sign)
