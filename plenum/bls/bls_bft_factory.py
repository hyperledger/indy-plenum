from common.serializers.serialization import multi_sig_store_serializer
from crypto.bls.bls_bft import BlsBft
from crypto.bls.bls_factory import BlsFactoryBft, BlsFactoryCrypto
from crypto.bls.bls_key_register import BlsKeyRegister
from crypto.bls.bls_multi_signature_verifier import MultiSignatureVerifier
from plenum.bls.bls_bft_plenum import BlsBftPlenum
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.bls.bls_key_register_pool_manager import BlsKeyRegisterPoolManager
from plenum.bls.bls_store import BlsStore


class BlsFactoryBftPlenum(BlsFactoryBft):
    def __init__(self, bls_factory_crypto: BlsFactoryCrypto, node):
        super().__init__(bls_factory_crypto)
        self._node = node

    def create_bls_store(self):
        return BlsStore(key_value_type=self._node.config.stateSignatureStorage,
                        data_location=self._node.dataLocation,
                        key_value_storage_name=self._node.config.stateSignatureDbName,
                        serializer=multi_sig_store_serializer)

    def _create_bls_key_register(self) -> BlsKeyRegister:
        return BlsKeyRegisterPoolManager(self._node.poolManager)

    def _create_bls_bft(self, bls_crypto, bls_crypto_registry, is_master) -> BlsBft:
        return BlsBftPlenum(bls_crypto,
                            bls_crypto_registry,
                            self._node.name,
                            is_master,
                            self._node.bls_store)


def create_default_bls_bft_factory(node):
    '''
    Creates a default BLS factory to instantiate BLS BFT classes.

    :param node: Node instance
    :return: BLS factory instance
    '''
    bls_crypto_factory = create_default_bls_crypto_factory(node.basedirpath,
                                                           node.name)
    return BlsFactoryBftPlenum(bls_crypto_factory,
                               node)
