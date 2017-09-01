from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsGroupParamsLoader
from crypto.bls.bls_factory import BlsFactory
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.bls_key_register import BlsKeyRegister
from crypto.bls.charm.bls_crypto_charm import BlsGroupParamsLoaderCharmHardcoded, BlsCryptoCharm
from plenum.bls.bls_bft_plenum import BlsBftPlenum
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile
from plenum.bls.bls_key_register_pool_ledger import BlsKeyRegisterPoolLedger


class BlsFactoryCharm(BlsFactory):
    def __init__(self, basedir=None, node_name=None):
        self.basedir = basedir
        self.node_name = node_name

    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        return BlsGroupParamsLoaderCharmHardcoded()

    def _create_key_manager(self, group_params) -> BlsKeyManager:
        assert self.basedir
        assert self.node_name
        return BlsKeyManagerFile(self.basedir, self.node_name)

    def _get_bls_crypto_class(self):
        return BlsCryptoCharm

    def _create_bls_crypto(self, sk, pk, group_params):
        return BlsCryptoCharm(sk=sk,
                              pk=pk,
                              params=group_params)

    def _create_bls_key_register(self) -> BlsKeyRegister:
        return BlsKeyRegisterPoolLedger()

    def _create_bls_bft(self, bls_crypto, bls_crypto_registry) -> BlsBft:
        return BlsBftPlenum(bls_crypto, bls_crypto_registry, self.node_name)


def create_default_bls_factory(basedir=None, node_name=None):
    '''
    Creates a default BLS factory to instantiate BLS-related classes.

    :param basedir: [optional] base dir; needed to save/load bls keys
    :param node_name: [optional] node's name; needed to save/load bls keys
    :return: BLS factory instance
    '''
    return BlsFactoryCharm(basedir, node_name)
