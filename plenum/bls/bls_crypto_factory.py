import os
from crypto.bls.bls_crypto import BlsGroupParamsLoader
from crypto.bls.bls_factory import BlsFactoryCrypto
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import BlsGroupParamsLoaderIndyCrypto, BlsCryptoSignerIndyCrypto, \
    BlsCryptoVerifierIndyCrypto
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile


# class BlsFactoryCharm(BlsFactoryPlenum):
#     def __init__(self, basedir=None, node_name=None):
#         self._basedir = basedir
#         self._node_name = node_name
#
#     def _create_group_params_loader(self) -> BlsGroupParamsLoader:
#         return BlsGroupParamsLoaderCharmHardcoded()
#
#     def _get_bls_crypto_class(self):
#         return BlsCryptoCharm
#
#     def _create_bls_crypto(self, sk, pk, group_params):
#         return BlsCryptoCharm(sk=sk, pk=pk, params=group_params)
#
#     def _create_key_manager(self, group_params) -> BlsKeyManager:
#         assert self._basedir
#         assert self._node_name
#         return BlsKeyManagerFile(self._basedir, self._node_name)


class BlsFactoryIndyCrypto(BlsFactoryCrypto):
    def __init__(self, keys_dir=None):
        self._keys_dir = keys_dir

    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        return BlsGroupParamsLoaderIndyCrypto()

    def _get_bls_crypto_signer_class(self):
        return BlsCryptoSignerIndyCrypto

    def _create_bls_crypto_signer(self, sk, pk, group_params):
        return BlsCryptoSignerIndyCrypto(sk=sk, pk=pk, params=group_params)

    def _create_bls_crypto_verifier(self, group_params):
        return BlsCryptoVerifierIndyCrypto(group_params)

    def _create_key_manager(self, group_params) -> BlsKeyManager:
        assert self._keys_dir
        return BlsKeyManagerFile(self._keys_dir)


def create_default_bls_crypto_factory(keys_dir=None):
    '''
    Creates a default BLS factory to instantiate BLS crypto classes.

    :param keys_dir: [optional] keys_dir; needed to save/load bls keys
    :return: BLS factory instance
    '''
    return BlsFactoryIndyCrypto(keys_dir)
