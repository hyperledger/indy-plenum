from crypto.bls.bls_crypto import BlsSerializer
from crypto.bls.bls_key_register import BlsKeyRegister
from ledger.ledger import Ledger
from plenum.common.constants import NODE, DATA, ALIAS, BLS_KEY, TYPE


class BlsKeyRegisterPoolLedger(BlsKeyRegister):
    def __init__(self, serializer: BlsSerializer):
        self._serializer = serializer
        self._bls_key_registry = {}

    def load_latest_keys(self, pool_ledger: Ledger):
        # TODO: Avoid iterating through all Ledger!
        for _, txn in pool_ledger.getAllTxn():
            if txn[TYPE] != NODE:
                continue
            if not DATA in txn:
                continue
            if not BLS_KEY in txn[DATA]:
                continue

            node_id = txn[DATA][ALIAS]
            bls_key_str = txn[DATA][BLS_KEY]
            bls_key = self._serializer.deserialize_from_str(bls_key_str)
            self._bls_key_registry[node_id] = bls_key

    def add_latest_key(self, node_id, bls_key_str: str):
        assert isinstance(bls_key_str, str)
        bls_key = self._serializer.deserialize_from_str(bls_key_str)
        self._bls_key_registry[node_id] = bls_key

    def remove_latest_key(self, node_id):
        self._bls_key_registry.pop(node_id, None)

    def get_latest_key(self, node_id):
        return self._bls_key_registry.get(node_id)

    def get_key_for_multisig(self, node_id, state_root):
        # TODO: keys may be changed with the latest batch!
        return self._bls_key_registry.get(node_id)
