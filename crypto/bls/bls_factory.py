from abc import ABCMeta, abstractmethod

from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_bft_replica import BlsBftReplica
from crypto.bls.bls_crypto import BlsGroupParamsLoader, BlsCryptoSigner, BlsCryptoVerifier
from crypto.bls.bls_key_manager import BlsKeyManager, LoadBLSKeyError
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.bls.bls_store import BlsStore
from stp_core.common.log import getlogger

logger = getlogger()


class BlsFactoryCrypto(metaclass=ABCMeta):
    def generate_bls_keys(self, seed=None) -> (str, str, str):
        return self._get_bls_crypto_signer_class().generate_keys(
            self._load_group_params(),
            seed)

    def generate_and_store_bls_keys(self, seed=None) -> (str, str):
        bls_key_manager = self._create_key_manager(self._load_group_params())

        sk, pk, key_proof = self.generate_bls_keys(seed)
        stored_sk, stored_pk = bls_key_manager.save_keys(sk, pk)

        return stored_pk, key_proof

    def create_bls_crypto_signer_from_saved_keys(self) -> BlsCryptoSigner:
        group_params = self._load_group_params()
        bls_key_manager = self._create_key_manager(group_params)
        sk, pk = bls_key_manager.load_keys()
        return self._create_bls_crypto_signer(sk, pk, group_params)

    def create_bls_crypto_verifier(self) -> BlsCryptoVerifier:
        group_params = self._load_group_params()
        return self._create_bls_crypto_verifier(group_params)

    def _load_group_params(self):
        return self._create_group_params_loader().load_group_params()

    @abstractmethod
    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        pass

    @abstractmethod
    def _create_key_manager(self, group_params) -> BlsKeyManager:
        pass

    @abstractmethod
    def _get_bls_crypto_signer_class(self):
        pass

    @abstractmethod
    def _create_bls_crypto_signer(self, sk, pk, group_params) -> BlsCryptoSigner:
        pass

    @abstractmethod
    def _create_bls_crypto_verifier(self, group_params) -> BlsCryptoVerifier:
        pass


class BlsFactoryBft(metaclass=ABCMeta):
    def __init__(self, bls_factory_crypto: BlsFactoryCrypto):
        self._bls_factory_crypto = bls_factory_crypto

    def create_bls_bft(self) -> BlsBft:
        try:
            bls_crypto_signer = self._bls_factory_crypto.create_bls_crypto_signer_from_saved_keys()
        except LoadBLSKeyError:
            bls_crypto_signer = None

        bls_crypto_verifier = self._bls_factory_crypto.create_bls_crypto_verifier()
        bls_key_register = self.create_bls_key_register()
        bls_store = self.create_bls_store()
        return BlsBft(bls_crypto_signer=bls_crypto_signer,
                      bls_crypto_verifier=bls_crypto_verifier,
                      bls_key_register=bls_key_register,
                      bls_store=bls_store)

    @abstractmethod
    def create_bls_bft_replica(self, is_master) -> BlsBftReplica:
        pass

    @abstractmethod
    def create_bls_store(self) -> BlsStore:
        pass

    @abstractmethod
    def create_bls_key_register(self) -> BlsKeyRegister:
        pass
