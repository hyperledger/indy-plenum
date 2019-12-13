from logging import getLogger

from indy_crypto.bls import VerKey

from crypto.bls.bls_key_register import BlsKeyRegister
from crypto.bls.indy_crypto.bls_crypto_indy_crypto import IndyCryptoBlsUtils
from plenum import config
from plenum.common.constants import BLS_KEY, NODE, ALIAS, DATA, BLS_KEY_PROOF
from plenum.common.tools import lazy_field
from plenum.common.txn_util import get_payload_data, get_type

logger = getLogger()


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

    def get_key_by_name(self, node_name, pool_state_root_hash=None) -> VerKey:
        return self._current_bls_keys.get(node_name, None)

    def _load_keys_for_root(self):
        keys = {}
        for _, txn in self._ledger.getAllTxn():
            if get_type(txn) != NODE:
                continue

            data = get_payload_data(txn)[DATA]
            node_name = data[ALIAS]

            if not config.VALIDATE_BLS_SIGNATURE_WITHOUT_KEY_PROOF and \
                    data.get(BLS_KEY_PROOF, None) is None:
                logger.warning("{} has no proof of possession for BLS public key.".format(node_name))
                keys[node_name] = None
                continue

            key_str = data.get(BLS_KEY, None)
            if key_str is None:
                keys[node_name] = None
                continue

            key_bls = IndyCryptoBlsUtils.bls_from_str(key_str, cls=VerKey)
            keys[node_name] = key_bls
        return keys
