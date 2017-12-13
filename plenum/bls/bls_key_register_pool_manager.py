from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.constants import BLS_KEY
from plenum.server.pool_manager import TxnPoolManager


class BlsKeyRegisterPoolManager(BlsKeyRegister):
    def __init__(self, pool_manager: TxnPoolManager):
        self._pool_manager = pool_manager
        # since pool state isn't changed very often, we cache keys corresponded
        # to the pool_state to not get them from the state trie each time
        self._current_bls_keys = {}  # {node_name : BLS key}
        self._current_pool_state_root_hash = None

    def get_pool_root_hash_committed(self):
        return self._pool_manager.state.committedHeadHash

    def get_key_by_name(self, node_name, pool_state_root_hash=None):
        if not pool_state_root_hash:
            pool_state_root_hash = self.get_pool_root_hash_committed()

        if self._current_pool_state_root_hash != pool_state_root_hash:
            self._current_pool_state_root_hash = pool_state_root_hash
            self._load_keys_for_root(pool_state_root_hash)

        return self._current_bls_keys.get(node_name)

    def _load_keys_for_root(self, pool_state_root_hash):
        self._current_bls_keys = {}
        for node_name in self._pool_manager.nodeReg.keys():
            node_nym = self._pool_manager.get_nym_by_name(node_name)
            data = self._pool_manager.reqHandler.get_node_data_for_root_hash(pool_state_root_hash, node_nym)
            if BLS_KEY in data:
                self._current_bls_keys[node_name] = data[BLS_KEY]
