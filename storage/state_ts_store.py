from plenum.common.constants import DOMAIN_LEDGER_ID
from stp_core.common.log import getlogger

logger = getlogger()


# TODO: Correctly handle cases for ledgers other than domain
class StateTsDbStorage():
    def __init__(self, name, storage):
        logger.debug("Initializing timestamp-rootHash storage")
        self._storage = storage
        self._name = name

    def __repr__(self):
        return self._name

    def get(self, timestamp: int, ledger_id: int = DOMAIN_LEDGER_ID):
        if ledger_id != DOMAIN_LEDGER_ID:
            return None

        value = self._storage.get(str(timestamp))
        return value

    def set(self, timestamp: int, root_hash: bytes, ledger_id: int = DOMAIN_LEDGER_ID):
        if ledger_id != DOMAIN_LEDGER_ID:
            return

        self._storage.put(str(timestamp), root_hash)

    def close(self):
        self._storage.close()

    def get_equal_or_prev(self, timestamp, ledger_id: int = DOMAIN_LEDGER_ID):
        if ledger_id != DOMAIN_LEDGER_ID:
            return None

        return self._storage.get_equal_or_prev(str(timestamp))

    def get_last_key(self, ledger_id: int = DOMAIN_LEDGER_ID):
        if ledger_id != DOMAIN_LEDGER_ID:
            return None

        return self._storage.get_last_key()
