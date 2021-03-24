from abc import abstractmethod
from typing import Dict

# TODO: move it to crypto repo


class Signer:
    """
    Interface that defines a sign method.
    """
    @property
    @abstractmethod
    def identifier(self):
        raise NotImplementedError

    @abstractmethod
    def sign(self, msg: Dict) -> Dict:
        raise NotImplementedError

    @property
    @abstractmethod
    def alias(self) -> str:
        raise NotImplementedError
