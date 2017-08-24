from abc import ABCMeta, abstractmethod
from collections import namedtuple
from typing import Any, Sequence

GroupParams = namedtuple('GroupParams',
                         'group_name, g')


class BlsGroupParamsLoader(metaclass=ABCMeta):
    @abstractmethod
    def load_group_params(self) -> GroupParams:
        pass


class BlsSerializer(metaclass=ABCMeta):
    def __init__(self, params: GroupParams):
        self._group_params = params

    @abstractmethod
    def serialize(self, obj: Any) -> bytes:
        pass

    @abstractmethod
    def deserialize(self, obj: bytes) -> Any:
        pass


class BlsCrypto(metaclass=ABCMeta):
    def __init__(self, sk: Any, pk: Any, params: GroupParams, serializer: BlsSerializer):
        assert sk
        assert pk
        self._sk = sk
        self.pk = pk
        self._serializer = serializer
        self._group_params = params

    @staticmethod
    @abstractmethod
    def generate_keys(params: GroupParams) -> (Any, Any):
        pass

    @abstractmethod
    def sign(self, message) -> Any:
        pass

    @abstractmethod
    def create_multi_sig(self, signatures: Sequence) -> Any:
        pass

    @abstractmethod
    def verify_sig(self, signature, message, pk) -> bool:
        pass

    @abstractmethod
    def verify_multi_sig(self, signature, message, pks: Sequence) -> bool:
        pass
