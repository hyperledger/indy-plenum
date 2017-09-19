from abc import ABCMeta, abstractmethod

from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare, Commit
from plenum.server.quorums import Quorums


class BlsBft(metaclass=ABCMeta):
    def __init__(self,
                 bls_crypto: BlsCrypto,
                 bls_key_register: BlsKeyRegister,
                 node_id,
                 is_master):
        self.bls_crypto = bls_crypto
        self.bls_key_register = bls_key_register
        self.node_id = node_id
        self.is_master = is_master

    @abstractmethod
    def validate_pre_prepare(self, pre_prepare: PrePrepare, sender):
        pass

    @abstractmethod
    def validate_prepare(self, prepare: Prepare, sender):
        pass

    @abstractmethod
    def validate_commit(self, commit: Commit, sender, state_root_hash):
        pass

    @abstractmethod
    def process_pre_prepare(self, pre_prepare: PrePrepare, sender):
        pass

    @abstractmethod
    def process_prepare(self, prepare: Prepare, sender):
        pass

    @abstractmethod
    def process_commit(self, commit: Commit, sender):
        pass

    @abstractmethod
    def process_order(self, key, state_root, pool_state_root, quorums, ledger_id):
        pass

    @abstractmethod
    def update_pre_prepare(self, pre_prepare_params, ledger_id):
        pass

    @abstractmethod
    def update_prepare(self, prepare_params, ledger_id):
        pass

    @abstractmethod
    def update_commit(self, commit_params, state_root_hash, ledger_id):
        pass

    @abstractmethod
    def gc(self, key_3PC):
        """
        Do some cleaning if needed

        :param key_3PC:
        """
        pass


class BlsValidationError(Exception):
    """
    BLS signature validation error
    """
