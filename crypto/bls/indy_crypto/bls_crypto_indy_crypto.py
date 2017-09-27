from typing import Sequence

import base58
from crypto.bls.bls_crypto import BlsCrypto, GroupParams, BlsGroupParamsLoader
from indy_crypto.bls import BlsEntity, Generator, VerKey, SignKey, Bls, Signature, MultiSignature

from crypto.bls.bls_multi_signature_verifier import MultiSignatureVerifier


class BlsGroupParamsLoaderIndyCrypto(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'generator'
        g = "7BRrJUcxuAomyoBC7YkvRD9TpFrcGoYAT9BQhhxuNB4FFjNPffimLywJViQRJAPnP97PQxCHTiEBTu6KuYV7trC4Ez3eRz7QSnKUwd5KqG9PxQaFaNaJyFv8uAXQgm3Q7nkEqjjKrCKdWmj89ZmAG848Ucn2v6bqhNmShEH9ARQqxhozXbmBy68oa6eh1vxs3DYenGgeWnjCCueBbR7vrMB9ATJBpCuPg25KWXjyh6KqnLsZfcRdst4NzuAmS8NzBPSvW6"
        return GroupParams(group_name, g)


class IndyCryptoBlsUtils:
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
            if len(seed_bytes) < BlsCryptoIndyCrypto.SEED_LEN:
                seed_bytes += b'0' * (BlsCryptoIndyCrypto.SEED_LEN - len(seed_bytes))
            assert (len(seed_bytes) == BlsCryptoIndyCrypto.SEED_LEN)

        return seed_bytes


class IndyCryptoMultiSigVerifier(MultiSignatureVerifier):

    def __init__(self, params: GroupParams):
        self._generator = \
            IndyCryptoBlsUtils\
            .bls_from_str(params.g, Generator)  # type: Generator

    def verify(self, signature: str, message: str, pks: Sequence[str]) -> bool:
        epks = [IndyCryptoBlsUtils.bls_from_str(p, VerKey) for p in pks]
        multi_signature = \
            IndyCryptoBlsUtils\
            .bls_from_str(signature, MultiSignature)  # type: MultiSignature
        message_bytes = IndyCryptoBlsUtils.msg_to_bls_bytes(message)
        return Bls.verify_multi_sig(multi_sig=multi_signature,
                                    message=message_bytes,
                                    ver_keys=epks,
                                    gen=self._generator)


class BlsCryptoIndyCrypto(BlsCrypto):
    SEED_LEN = 48

    def __init__(self, sk: str, pk: str, params: GroupParams):
        super().__init__(sk, pk, params)
        self._sk_bls = IndyCryptoBlsUtils.bls_from_str(sk, SignKey)
        self._pk_bls = IndyCryptoBlsUtils.bls_from_str(pk, VerKey)
        self._generator = \
            IndyCryptoBlsUtils\
            .bls_from_str(params.g, Generator)  # type: Generator
        self._multi_sig_verifier = IndyCryptoMultiSigVerifier(params)

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

    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        sigs = [IndyCryptoBlsUtils.bls_from_str(s, Signature) for s in signatures]
        bts = MultiSignature.new(sigs)
        return IndyCryptoBlsUtils.bls_to_str(bts)

    def verify_sig(self, signature: str, message: str, pk: str) -> bool:
        return Bls.verify(IndyCryptoBlsUtils.bls_from_str(signature, Signature),
                          IndyCryptoBlsUtils.msg_to_bls_bytes(message),
                          IndyCryptoBlsUtils.bls_from_str(pk, VerKey),
                          self._generator)

    def verify_multi_sig(self, signature: str, message: str, pks: Sequence[str]):
        return self._multi_sig_verifier.verify(signature, message, pks)
