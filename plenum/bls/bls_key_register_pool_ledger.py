from crypto.bls.bls_key_register import BlsKeyRegister
from plenum.common.constants import BLS_KEY, NODE, ALIAS, DATA
from plenum.common.tools import lazy_field
from plenum.common.txn_util import get_payload_data, get_type


class BlsKeyRegisterPoolLedger(BlsKeyRegister):
    """
    Ledger-based implementation of BlsKeyRegister
    """

    def __init__(self, ledger):
        self._ledger = ledger
        # Not supported methods

    def get_pool_root_hash_committed(self):
        raise NotImplementedError()

    @lazy_field
    def _current_bls_keys(self):
        return self._load_keys_for_root()  # {node_name : BLS key}

    def get_key_by_name(self, node_name, pool_state_root_hash=None):
        return self._current_bls_keys.get(node_name)

    def _load_keys_for_root(self):
        keys = {}
        for _, txn in self._ledger.getAllTxn():
            if get_type(txn) == NODE:
                data = get_payload_data(txn)[DATA]
                alias = data[ALIAS]
                blskey = data.get(BLS_KEY)
                keys[alias] = blskey
        return keys
