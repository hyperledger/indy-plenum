from abc import ABCMeta, abstractmethod

from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare


class BlsBft(metaclass=ABCMeta):
    def __init__(self, bls_crypto: BlsCrypto, bls_key_register: BlsKeyRegister):
        self.bls_crypto = bls_crypto
        self.bls_key_register = bls_key_register

    @abstractmethod
    def validate_pre_prepare(self, pre_prepare: PrePrepare):
        pass

    @abstractmethod
    def validate_prepare(self, pre_prepare: Prepare):
        pass

    @abstractmethod
    def sign_state(self, state):
        pass

    @abstractmethod
    def save_multi_sig(self, multi_sig):
        pass
