from abc import ABCMeta, abstractmethod
from typing import Sequence

from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.server.quorums import Quorums


class BlsBft(metaclass=ABCMeta):
    def __init__(self, bls_crypto: BlsCrypto, bls_key_register: BlsKeyRegister, node_id):
        self.bls_crypto = bls_crypto
        self.bls_key_register = bls_key_register
        self.node_id = node_id

    @abstractmethod
    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        pass

    @abstractmethod
    def validate_prepare(self, prepare: Prepare, sender):
        pass

    @abstractmethod
    def validate_commit(self, key_3PC, commit: Commit, sender, state_root):
        pass

    @abstractmethod
    def sign_state(self, state_root: str) -> str:
        pass

    @abstractmethod
    def calculate_multi_sig(self, key_3PC, quorums: Quorums) -> str:
        pass

    @abstractmethod
    def save_multi_sig_local(self, multi_sig: str, state_root, key_3PC):
        pass

    @abstractmethod
    def save_multi_sig_shared(self, pre_prepare: PrePrepare, key_3PC):
        pass

    @abstractmethod
    def gc(self, key_3PC):
        pass