import logging
from logging import getLogger
from typing import Sequence, Optional

import base58
from indy_crypto import IndyCryptoError

from crypto.bls.bls_crypto import GroupParams, BlsGroupParamsLoader, BlsCryptoVerifier, BlsCryptoSigner
from indy_crypto.bls import BlsEntity, Generator, VerKey, SignKey, Bls, \
    Signature, MultiSignature, ProofOfPossession

logging.getLogger("indy_crypto").setLevel(logging.WARNING)
logger = getLogger()


class BlsGroupParamsLoaderIndyCrypto(BlsGroupParamsLoader):
    def load_group_params(self) -> GroupParams:
        group_name = 'generator'
        g = "3LHpUjiyFC2q2hD7MnwwNmVXiuaFbQx2XkAFJWzswCjgN1utjsCeLzHsKk1nJvFEaS4fcrUmVAkdhtPCYbrVyATZcmzwJReTcJqwqBCPTmTQ9uWPwz6rEncKb2pYYYFcdHa8N17HzVyTqKfgPi4X9pMetfT3A5xCHq54R2pDNYWVLDX"
        return GroupParams(group_name, g)


class IndyCryptoBlsUtils:
    SEED_LEN = 32

    @staticmethod
    def bls_to_str(v: BlsEntity) -> str:
        try:
            return base58.b58encode(v.as_bytes()).decode("utf-8")
        except ValueError:
            logger.warning('BLS: BLS Entity can not be encoded as base58')

    @staticmethod
    def bls_from_str(v: str, cls) -> Optional[BlsEntity]:
        try:
            bts = base58.b58decode(v)
        except ValueError:
            logger.warning('BLS: value {} can not be decoded to base58'.format(v))
            return None

        try:
            return cls.from_bytes(bts)
        except IndyCryptoError as e:
            logger.warning('BLS: Indy Crypto error: {}'.format(e))
            return None

    @staticmethod
    def bls_pk_from_str(v: str) -> Optional[VerKey]:
        return IndyCryptoBlsUtils.bls_from_str(v, VerKey)

    @staticmethod
    def prepare_seed(seed):
        seed_bytes = None
        if isinstance(seed, str):
            seed_bytes = seed.encode()
        if isinstance(seed, (bytes, bytearray)):
            seed_bytes = seed

        # TODO: FIXME: indy-crypto supports 32-bit seeds only
        if seed_bytes:
            if len(seed_bytes) < IndyCryptoBlsUtils.SEED_LEN:
                seed_bytes += b'0' * (IndyCryptoBlsUtils.SEED_LEN - len(seed_bytes))
            assert (len(seed_bytes) >= IndyCryptoBlsUtils.SEED_LEN)

        return seed_bytes


class BlsCryptoVerifierIndyCrypto(BlsCryptoVerifier):
    def __init__(self, params: GroupParams):
        self._generator = \
            IndyCryptoBlsUtils.bls_from_str(params.g, Generator)  # type: Generator

    def verify_sig(self, signature: str, message: bytes, bls_pk: Optional[VerKey]) -> bool:
        bls_signature = IndyCryptoBlsUtils.bls_from_str(signature, Signature)
        if bls_signature is None:
            return False
        if bls_pk is None:
            return False
        return Bls.verify(bls_signature,
                          message,
                          bls_pk,
                          self._generator)

    def verify_multi_sig(self, signature: str, message: bytes, pks: Sequence[Optional[VerKey]]) -> bool:
        # TODO: is it expected that we return False if one of the keys is None?
        if None in pks:
            return False

        multi_signature = \
            IndyCryptoBlsUtils.bls_from_str(signature, MultiSignature)  # type: MultiSignature
        if multi_signature is None:
            return False

        return Bls.verify_multi_sig(multi_sig=multi_signature,
                                    message=message,
                                    ver_keys=pks,
                                    gen=self._generator)

    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        sigs = [IndyCryptoBlsUtils.bls_from_str(s, Signature) for s in signatures]
        bts = MultiSignature.new(sigs)
        return IndyCryptoBlsUtils.bls_to_str(bts)

    def verify_key_proof_of_possession(self, key_proof: Optional[ProofOfPossession], bls_pk: Optional[VerKey]) -> bool:
        if None in [key_proof, bls_pk]:
            return False
        return Bls.verify_pop(key_proof,
                              bls_pk,
                              self._generator)


class BlsCryptoSignerIndyCrypto(BlsCryptoSigner):
    def __init__(self, sk: SignKey, pk: VerKey, params: GroupParams):
        self._sk = sk  # type: SignKey
        self.pk = pk  # type: VerKey
        self._generator = \
            IndyCryptoBlsUtils.bls_from_str(params.g, Generator)  # type: Generator

    @staticmethod
    def generate_keys(params: GroupParams, seed=None) -> (SignKey, VerKey, ProofOfPossession):
        seed = IndyCryptoBlsUtils.prepare_seed(seed)
        gen = IndyCryptoBlsUtils.bls_from_str(params.g, Generator)
        sk = SignKey.new(seed)
        vk = VerKey.new(gen, sk)
        key_proof = ProofOfPossession.new(ver_key=vk, sign_key=sk)
        return sk, vk, key_proof

    @staticmethod
    def generate_key_proof(sk: SignKey, pk: VerKey) -> ProofOfPossession:
        return ProofOfPossession.new(ver_key=pk, sign_key=sk)

    def sign(self, message: bytes) -> str:
        sign = Bls.sign(message, self._sk)
        return IndyCryptoBlsUtils.bls_to_str(sign)
