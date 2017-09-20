from common.serializers.serialization import multi_sig_store_serializer
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_crypto import BlsGroupParamsLoader
from crypto.bls.bls_factory import BlsFactory
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.bls_key_register import BlsKeyRegister
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import BlsCryptoIndyCrypto, BlsGroupParamsLoaderIndyCrypto
from plenum.bls.bls_bft_plenum import BlsBftPlenum
from plenum.bls.bls_key_manager_file import BlsKeyManagerFile
from plenum.bls.bls_key_register_pool_ledger import BlsKeyRegisterPoolLedger
from plenum.bls.bls_store import BlsStore


class BlsFactoryPlenum(BlsFactory):
    def __init__(self, basedir=None, data_location=None, node_name=None, config=None):
        self.basedir = basedir
        self.data_location = data_location
        self.node_name = node_name
        self.config = config

    def create_bls_store(self):
        return BlsStore(key_value_type=self.config.stateSignatureStorage,
                        data_location=self.data_location,
                        key_value_storage_name=self.config.stateSignatureDbName,
                        serializer=multi_sig_store_serializer)

    def _create_key_manager(self, group_params) -> BlsKeyManager:
        assert self.basedir
        assert self.node_name
        return BlsKeyManagerFile(self.basedir, self.node_name)

    def _create_bls_key_register(self) -> BlsKeyRegister:
        return BlsKeyRegisterPoolLedger()

    def _create_bls_bft(self, bls_crypto, bls_crypto_registry, pool_state, bls_store, is_master) -> BlsBft:
        return BlsBftPlenum(bls_crypto, bls_crypto_registry, self.node_name, is_master, pool_state, bls_store)


# class BlsFactoryCharm(BlsFactoryPlenum):
#     def __init__(self, basedir=None, data_location=None, node_name=None, config=None):
#         super().__init__(basedir, data_location, node_name, config)
#
#     def _create_group_params_loader(self) -> BlsGroupParamsLoader:
#         return BlsGroupParamsLoaderCharmHardcoded()
#
#     def _get_bls_crypto_class(self):
#         return BlsCryptoCharm
#
#     def _create_bls_crypto(self, sk, pk, group_params):
#         return BlsCryptoCharm(sk=sk, pk=pk, params=group_params)


class BlsFactoryIndyCrypto(BlsFactoryPlenum):
    def __init__(self, basedir=None, data_location=None, node_name=None, config=None):
        super().__init__(basedir, data_location, node_name, config)

    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        return BlsGroupParamsLoaderIndyCrypto()

    def _get_bls_crypto_class(self):
        return BlsCryptoIndyCrypto

    def _create_bls_crypto(self, sk, pk, group_params):
        return BlsCryptoIndyCrypto(sk=sk, pk=pk, params=group_params)


def create_default_bls_factory(basedir=None, node_name=None, data_location=None, config=None):
    '''
    Creates a default BLS factory to instantiate BLS-related classes.

    :param basedir: [optional] base dir; needed to save/load bls keys
    :param node_name: [optional] node's name; needed to save/load bls keys
    :return: BLS factory instance
    '''
    return BlsFactoryIndyCrypto(basedir, data_location, node_name, config)
