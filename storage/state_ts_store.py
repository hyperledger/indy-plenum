from typing import Dict

from plenum.common.constants import DOMAIN_LEDGER_ID
from storage.kv_store import KeyValueStorage
from stp_core.common.log import getlogger

logger = getlogger()


class StateTsDbStorage():
    def __init__(self, name: str, storages: Dict[int, KeyValueStorage]):
        logger.debug("Initializing timestamp-rootHash storage")
        self._storages = storages
        self._name = name

    def __repr__(self):
        return self._name

    def get(self, timestamp: int, ledger_id: int = DOMAIN_LEDGER_ID):
        storage = self._storages.get(ledger_id)
        if storage is None:
            return None

        value = storage.get(str(timestamp))
        return value

    def set(self, timestamp: int, root_hash: bytes, ledger_id: int = DOMAIN_LEDGER_ID):
        storage = self._storages.get(ledger_id)
        if storage is None:
            return

        storage.put(str(timestamp), root_hash)

    def close(self):
        for storage in self._storages.values():
            storage.close()

    def get_equal_or_prev(self, timestamp, ledger_id: int = DOMAIN_LEDGER_ID):
        storage = self._storages.get(ledger_id)
        if storage is None:
            return None

        return storage.get_equal_or_prev(str(timestamp))

    def get_last_key(self, ledger_id: int = DOMAIN_LEDGER_ID):
        storage = self._storages.get(ledger_id)
        if storage is None:
            return None

        return storage.get_last_key()
