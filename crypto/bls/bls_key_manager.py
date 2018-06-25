from abc import ABCMeta, abstractmethod

from common.exceptions import PlenumTypeError


class LoadBLSKeyError(RuntimeError):
    pass


class BlsKeyManager(metaclass=ABCMeta):
    def save_keys(self, sk: str, pk: str):
        if not isinstance(sk, str):
            raise PlenumTypeError('sk', sk, str)
        if not isinstance(pk, str):
            raise PlenumTypeError('pk', pk, str)

        self._save_secret_key(sk)
        self._save_public_key(pk)
        return sk, pk

    def load_keys(self) -> (str, str):
        try:
            sk = self._load_secret_key()
            pk = self._load_public_key()
        except BaseException as e:
            raise LoadBLSKeyError(e)

        return sk, pk

    @abstractmethod
    def _save_secret_key(self, sk: str):
        pass

    @abstractmethod
    def _save_public_key(self, pk: str):
        pass

    @abstractmethod
    def _load_secret_key(self) -> str:
        pass

    @abstractmethod
    def _load_public_key(self) -> str:
        pass
