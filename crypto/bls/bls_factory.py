from abc import ABCMeta, abstractmethod
from typing import Any

from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsCrypto, BlsGroupParamsLoader, BlsSerializer
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.bls_key_register import BlsKeyRegister


class BlsFactory(metaclass=ABCMeta):
    def generate_bls_keys(self, seed=None) -> Any:
        group_params_loader = self._create_group_params_loader()
        group_params = group_params_loader.load_group_params()
        bls_crypto_class = self._get_bls_crypto_class()
        return bls_crypto_class.generate_keys(group_params, seed)

    def generate_bls_keys_as_str(self, seed=None) -> Any:
        bls_sk, bls_pk = self.generate_bls_keys(seed)
        bls_serializer = self.create_serializer()
        return bls_serializer.serialize_to_str(bls_pk)

    def generate_and_store_bls_keys(self, seed=None) -> Any:
        group_params_loader = self._create_group_params_loader()
        group_params = group_params_loader.load_group_params()
        serializer = self._create_serializer(group_params)
        bls_key_manager = self._create_key_manager(serializer, group_params)

        sk, pk = self.generate_bls_keys(seed)
        stored_sk, stored_pk = bls_key_manager.save_keys(sk, pk)

        return stored_pk

    def create_bls_crypto_from_saved_keys(self) -> BlsCrypto:
        group_params_loader = self._create_group_params_loader()
        group_params = group_params_loader.load_group_params()
        serializer = self._create_serializer(group_params)
        bls_key_manager = self._create_key_manager(serializer, group_params)
        sk, pk = bls_key_manager.load_keys()
        return self._create_bls_crypto(sk, pk, group_params, serializer)

    def create_serializer(self) -> BlsSerializer:
        group_params_loader = self._create_group_params_loader()
        group_params = group_params_loader.load_group_params()
        return self._create_serializer(group_params)

    def create_bls_bft(self, bls_crypto, bls_crypto_registry) -> BlsBft:
        group_params_loader = self._create_group_params_loader()
        group_params = group_params_loader.load_group_params()
        serializer = self._create_serializer(group_params)
        bls_key_manager = self._create_key_manager(serializer, group_params)
        sk, pk = bls_key_manager.load_keys()

        bls_crypto = self._create_bls_crypto(sk, pk, group_params, serializer)
        bls_key_register = self._create_bls_key_register(serializer)

        return self._create_bls_bft(bls_crypto, bls_key_register)

    @abstractmethod
    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        pass

    @abstractmethod
    def _create_serializer(self, group_params) -> BlsSerializer:
        pass

    @abstractmethod
    def _create_key_manager(self, serializer, group_params) -> BlsKeyManager:
        pass

    @abstractmethod
    def _get_bls_crypto_class(self):
        pass

    @abstractmethod
    def _create_bls_crypto(self, sk, pk, group_params, serializer):
        pass

    @abstractmethod
    def _create_bls_key_register(self, serializer) -> BlsKeyRegister:
        pass

    @abstractmethod
    def _create_bls_bft(self, bls_crypto, bls_key_register) -> BlsBft:
        pass
