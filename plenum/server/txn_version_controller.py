from plenum.common.txn_util import get_version


class TxnVersionController:

    @property
    def version(self):
        return None

    def update_version(self, txn):
        pass

    def get_txn_version(self, txn):
        return get_version(txn)

    def get_pool_version(self, timestamp):
        return None
