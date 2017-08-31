from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.messages.node_messages import PrePrepare, Prepare


class BlsBftPlenum(BlsBft):
    def __init__(self, bls_crypto: BlsCrypto, bls_key_register: BlsKeyRegister):
        super().__init__(bls_crypto, bls_key_register)

    def validate_pre_prepare(self, pre_prepare: PrePrepare):
        pass

    def validate_prepare(self, pre_prepare: Prepare):
        pass

    def sign_state(self, state):
        pass

    def save_multi_sig(self, multi_sig):
        pass
