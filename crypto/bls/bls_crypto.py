from abc import ABCMeta, abstractmethod
from collections import namedtuple
from typing import Sequence

GroupParams = namedtuple('GroupParams',
                         'group_name, g')


class BlsGroupParamsLoader(metaclass=ABCMeta):
    @abstractmethod
    def load_group_params(self) -> GroupParams:
        pass


class BlsCryptoSigner(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def generate_keys(params: GroupParams, seed=None) -> (object, object, object):
        pass

    @staticmethod
    @abstractmethod
    def generate_key_proof(sk: object, pk: object) -> object:
        pass

    @abstractmethod
    def sign(self, message: bytes) -> str:
        pass


class BlsCryptoVerifier(metaclass=ABCMeta):
    @abstractmethod
    def create_multi_sig(self, signatures: Sequence[str]) -> str:
        pass

    @abstractmethod
    def verify_sig(self, signature: str, message: bytes, pk: object) -> bool:
        pass

    @abstractmethod
    def verify_multi_sig(self, signature: str, message: bytes, pks: Sequence[object]) -> bool:
        pass

    @abstractmethod
    def verify_key_proof_of_possession(self, key_proof: object, pk: object) -> bool:
        pass
