from crypto.bls.bls_crypto import BlsGroupParamsLoader, BlsSerializer
from crypto.bls.bls_factory import BlsFactory
from crypto.bls.bls_key_manager import BlsKeyManager
from crypto.bls.charm.bls_crypto_charm import BlsGroupParamsLoaderCharmHardcoded, BlsSerializerCharm, BlsCryptoCharm
from plenum.server.bls.bls_key_manager_file import BlsKeyManagerFile


class BlsFactoryCharm(BlsFactory):
    def __init__(self, basedir, node_name):
        self.basedir = basedir
        self.node_name = node_name

    def _create_group_params_loader(self) -> BlsGroupParamsLoader:
        return BlsGroupParamsLoaderCharmHardcoded()

    def _create_serializer(self, group_params) -> BlsSerializer:
        return BlsSerializerCharm(group_params)

    def _create_key_manager(self, serializer, group_params) -> BlsKeyManager:
        return BlsKeyManagerFile(serializer, self.basedir, self.node_name)

    def _get_bls_crypto_class(self):
        return BlsCryptoCharm

    def _create_bls_crypto(self, sk, pk, group_params, serializer):
        return BlsCryptoCharm(sk=sk,
                              pk=pk,
                              params=group_params,
                              serializer=serializer)
