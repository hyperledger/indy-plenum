from abc import ABCMeta, abstractmethod

from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsCrypto, BlsGroupParamsLoader
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.bls.bls_store import BlsStore


class BlsFactory(metaclass=ABCMeta):
    def generate_bls_keys(self, seed=None) -> str:
        return self._get_bls_crypto_class().generate_keys(
            self._load_group_params(),
            seed)

    def generate_and_store_bls_keys(self, seed=None) -> str:
        bls_key_manager = self._create_key_manager(
            self._load_group_params())

        sk, pk = self.generate_bls_keys(seed)
        stored_sk, stored_pk = bls_key_manager.save_keys(sk, pk)

        return stored_pk

    def create_bls_crypto_from_saved_keys(self) -> BlsCrypto:
        group_params = self._load_group_params()
        bls_key_manager = self._create_key_manager(group_params)
        sk, pk = bls_key_manager.load_keys()
        return self._create_bls_crypto(sk, pk, group_params)

    def create_bls_bft(self, is_master, bls_store) -> BlsBft:
        bls_crypto = self.create_bls_crypto_from_saved_keys()
        bls_key_register = self._create_bls_key_register()
        return self._create_bls_bft(bls_crypto, bls_key_register, bls_store, is_master)

    @abstractmethod
    def create_bls_store(self) -> BlsStore:
        pass

    def _load_group_params(self):
        return self._create_group_params_loader().load_group_params()

    @abstractmethod
    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        pass

    @abstractmethod
    def _create_key_manager(self, group_params) -> BlsKeyManager:
        pass

    @abstractmethod
    def _get_bls_crypto_class(self):
        pass

    @abstractmethod
    def _create_bls_crypto(self, sk, pk, group_params):
        pass

    @abstractmethod
    def _create_bls_key_register(self) -> BlsKeyRegister:
        pass

    @abstractmethod
    def _create_bls_bft(self, bls_crypto, bls_key_register, bls_store, is_master) -> BlsBft:
        pass
