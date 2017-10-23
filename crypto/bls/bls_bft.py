from abc import ABCMeta

from crypto.bls.bls_crypto import BlsCryptoSigner, BlsCryptoVerifier
from crypto.bls.bls_key_register import BlsKeyRegister


class BlsBft(metaclass=ABCMeta):

    PPR_NO_BLS_MULTISIG_STATE = 0
    PPR_BLS_MULTISIG_WRONG = 1
    CM_BLS_SIG_WRONG = 2

    def __init__(self,
                 bls_crypto_signer: BlsCryptoSigner,
                 bls_crypto_verifier: BlsCryptoVerifier,
                 bls_key_register: BlsKeyRegister,
                 bls_store):
        self.bls_key_register = bls_key_register
        self.bls_crypto_signer = bls_crypto_signer
        self.bls_crypto_verifier = bls_crypto_verifier
        self.bls_store = bls_store

    def can_sign_bls(self):
        return self.bls_crypto_signer is not None
