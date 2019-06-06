from logging import getLogger

from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.constants import BLS_KEY, BLS_KEY_PROOF, ALIAS
from plenum.server.pool_manager import TxnPoolManager

logger = getLogger()


class BlsKeyRegisterPoolManager(BlsKeyRegister):
    def __init__(self, node):
        self._node = node
        # since pool state isn't changed very often, we cache keys corresponded
        # to the pool_state to not get them from the state trie each time
        self._current_bls_keys = {}  # {node_name : BLS key}
        self._current_pool_state_root_hash = None

    def get_pool_root_hash_committed(self):
        return self._node.poolManager.state.committedHeadHash

    def get_key_by_name(self, node_name, pool_state_root_hash=None):
        if not pool_state_root_hash:
            pool_state_root_hash = self.get_pool_root_hash_committed()

        if self._current_pool_state_root_hash != pool_state_root_hash:
            self._current_pool_state_root_hash = pool_state_root_hash
            self._load_keys_for_root(pool_state_root_hash)

        return self._current_bls_keys.get(node_name, None)

    def _load_keys_for_root(self, pool_state_root_hash):
        self._current_bls_keys = {}
        for data in self._node.write_manager.get_all_node_data_for_root_hash(
                pool_state_root_hash):
            node_name = data[ALIAS]
            if BLS_KEY in data:
                if not self._node.poolManager.config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF and \
                        data.get(BLS_KEY_PROOF, None) is None:
                    logger.warning("{} has no proof of possession for BLS public key.".format(node_name))
                    self._current_bls_keys[node_name] = None
                else:
                    self._current_bls_keys[node_name] = data[BLS_KEY]
