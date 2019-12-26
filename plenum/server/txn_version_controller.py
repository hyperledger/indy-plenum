from plenum.common.txn_util import get_payload_txn_version


class TxnVersionController:

    @property
    def version(self):
        return None

    def update_version(self, txn):
        pass

    def get_txn_version(self, txn):
        version = get_payload_txn_version(txn)
        return "1" if version is None else version

    def get_pool_version(self, timestamp):
        return None
