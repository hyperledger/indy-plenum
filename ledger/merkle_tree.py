from abc import abstractmethod, ABCMeta
from typing import Tuple

from ledger.hash_stores.hash_store import HashStore


class MerkleTree(metaclass=ABCMeta):
    """
    Interface to be implemented by all Merkle Trees.
    """

    @abstractmethod
    def append(self, new_leaf):
        """
        """

    @abstractmethod
    def merkle_tree_hash(self, start, end):
        """
        """

    @abstractmethod
    def consistency_proof(self, first, second):
        """
        """

    @abstractmethod
    def inclusion_proof(self, start, end):
        """
        """

    @abstractmethod
    def get_tree_head(self, seq=None):
        """
        """

    @property
    @abstractmethod
    def hashes(self) -> Tuple[bytes]:
        """
        """

    @property
    @abstractmethod
    def root_hash(self) -> bytes:
        """
        """

    @property
    @abstractmethod
    def root_hash_hex(self) -> bytes:
        """
        """

    @property
    @abstractmethod
    def tree_size(self) -> int:
        """
        """

    @property
    @abstractmethod
    def leafCount(self) -> int:
        """
        """

    @property
    @abstractmethod
    def nodeCount(self) -> int:
        """
        """

    @abstractmethod
    def verify_consistency(self, expectedLeafCount) -> bool:
        """
        """

    # TODO: do we need a separate interface/mixin for a Merkle Tree with Hash Store?
    @property
    @abstractmethod
    def hashStore(self) -> HashStore:
        """
        """

    @property
    @abstractmethod
    def reset(self):
        """
        """
