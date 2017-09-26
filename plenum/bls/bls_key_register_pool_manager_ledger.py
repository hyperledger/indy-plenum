from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.constants import BLS_KEY, TXN_TYPE, NODE, ALIAS, DATA
from plenum.server.pool_manager import TxnPoolManager


class BlsKeyRegisterPoolManagerLedger(BlsKeyRegister):
    """
    Ledger-based implementation of  BlsKeyRegister
    """

    def __init__(self, pool_manager: TxnPoolManager):
        self._pool_manager = pool_manager
        self._current_bls_keys = None  # {node_name : BLS key}
        # Not supported methods
        self.get_pool_root_hash_committed = None

    def get_key_by_name(self, node_name, pool_state_root_hash=None):
        if self._current_bls_keys is None:
            self._current_bls_keys = self._load_keys_for_root()
        return self._current_bls_keys.get(node_name)

    def _load_keys_for_root(self):
        keys = {}
        for _, txn in self._pool_manager.ledger.getAllTxn():
            if txn[TXN_TYPE] == NODE:
                data = txn[DATA]
                alias = data[ALIAS]
                blskey = data.get(BLS_KEY)
                keys[alias] = blskey
        return keys
