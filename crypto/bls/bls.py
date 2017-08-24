from abc import ABCMeta, abstractmethod
from typing import Any, Sequence, Tuple


class BlsSignature(metaclass=ABCMeta):
    @abstractmethod
    def generate_keys(self) -> (Any, Any):
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
    def verify_multi_sig(self, signature, messages_with_pk: Sequence) -> bool:
        pass
