from abc import ABCMeta
from typing import Sequence


class MultiSignatureVerifier(metaclass=ABCMeta):
    """
    Abstract multi signature verifier, to be used in
    places where full bls support is not required.
    """

    def verify(self, signature: str, message: str, pks: Sequence[str]) -> bool:
        """
        Verify multi signature
        """
        pass
