from abc import ABCMeta, abstractmethod
from typing import Any

from crypto.bls.bls_crypto import BlsSerializer


class LoadBLSKeyError(RuntimeError):
    pass


class BlsKeyManager(metaclass=ABCMeta):
    def __init__(self, serializer: BlsSerializer):
        self._serializer = serializer

    def save_keys(self, sk, pk):
        sk = self._serializer.serialize(sk)
        pk = self._serializer.serialize(pk)
        self._save_secret_key(sk)
        self._save_public_key(pk)
        return sk, pk

    def load_keys(self) -> (Any, Any):
        try:
            sk = self._load_secret_key()
            pk = self._load_public_key()
        except BaseException as e:
            raise LoadBLSKeyError(e)

        sk = self._serializer.deserialize(sk)
        pk = self._serializer.deserialize(pk)
        return sk, pk

    @abstractmethod
    def _save_secret_key(self, sk: bytes):
        pass

    @abstractmethod
    def _save_public_key(self, pk: bytes):
        pass

    @abstractmethod
    def _load_secret_key(self) -> bytes:
        pass

    @abstractmethod
    def _load_public_key(self) -> bytes:
        pass
