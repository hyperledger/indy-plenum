from abc import ABCMeta, abstractmethod
from typing import Any

from crypto.bls.bls_crypto import BlsCrypto, BlsGroupParamsLoader, BlsSerializer
from crypto.bls.bls_key_manager import BlsKeyManager


class BlsFactory(metaclass=ABCMeta):
    def create_bls_keys(self) -> Any:
        group_params_loader = self._create_group_params_loader()
        group_params = group_params_loader.load_group_params()

        serializer = self._create_serializer(group_params)
        bls_key_manager = self._create_key_manager(serializer, group_params)
        bls_crypto_class = self._get_bls_crypto_class()

        sk, pk = bls_crypto_class.generate_keys(group_params)
        stored_sk, stored_pk = bls_key_manager.save_keys(sk, pk)

        return stored_pk

    def create_bls_crypto(self) -> BlsCrypto:
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
