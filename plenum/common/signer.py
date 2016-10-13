from abc import abstractproperty, abstractmethod
from typing import Dict

from plenum.common.types import Identifier


class Signer:
    """
    Interface that defines a sign method.
    """
    @abstractproperty
    def identifier(self) -> Identifier:
        raise NotImplementedError

    @abstractmethod
    def sign(self, msg: Dict) -> Dict:
        raise NotImplementedError

    @abstractproperty
    def alias(self) -> str:
        raise NotImplementedError


