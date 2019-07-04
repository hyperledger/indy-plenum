from typing import Dict, Optional

from common.exceptions import LogicError
from plenum.common.constants import BLS_LABEL, TS_LABEL, IDR_CACHE_LABEL, ATTRIB_LABEL, SEQ_NO_DB_LABEL
from plenum.common.ledger import Ledger
from state.state import State


class DatabaseManager():
    def __init__(self):
        self.databases = {}  # type: Dict[int, Database]
        self.stores = {}
        self.trackers = {}
        self._init_db_list()

    def _init_db_list(self):
        self._ledgers = {lid: db.ledger for lid, db in self.databases.items()}
        self._states = {lid: db.state for lid, db in self.databases.items() if db.state}

    def register_new_database(self, lid, ledger: Ledger, state: Optional[State] = None, taa_acceptance_required=True):
        if lid in self.databases:
            raise LogicError('Trying to add already existing database')
        self.databases[lid] = Database(ledger, state, taa_acceptance_required=taa_acceptance_required)
        self._init_db_list()

    def get_database(self, lid):
        if lid not in self.databases:
            return None
        return self.databases[lid]

    def get_ledger(self, lid):
        if lid not in self.databases:
            return None
        return self.databases[lid].ledger

    def get_state(self, lid):
        if lid not in self.databases:
            return None
        return self.databases[lid].state

    def get_tracker(self, lid):
        if lid not in self.trackers:
            return None
        return self.trackers[lid]

    def register_new_store(self, label, store):
        if label in self.stores:
            raise LogicError('Trying to add already existing store')
        self.stores[label] = store

    def register_new_tracker(self, lid, tracker):
        if lid in self.trackers:
            raise LogicError("Trying to add already existing tracker")
        self.trackers[lid] = tracker

    def get_store(self, label):
        if label not in self.stores:
            return None
        return self.stores[label]

    def is_taa_acceptance_required(self, lid):
        if lid not in self.databases:
            return False
        return self.databases[lid].taa_acceptance_required

    @property
    def states(self):
        return self._states

    @property
    def ledgers(self):
        return self._ledgers

    @property
    def bls_store(self):
        return self.get_store(BLS_LABEL)

    @property
    def ts_store(self):
        return self.get_store(TS_LABEL)

    @property
    def idr_cache(self):
        return self.get_store(IDR_CACHE_LABEL)

    @property
    def attribute_store(self):
        return self.get_store(ATTRIB_LABEL)

    @property
    def seq_no_db(self):
        return self.get_store(SEQ_NO_DB_LABEL)

    # ToDo: implement it and use on close all KV stores
    def close(self):
        # Close all states
        for state in self.states.values():
            state.close()

        # Close all stores
        for store in self.stores.values():
            store.close()


class Database:
    def __init__(self, ledger, state, taa_acceptance_required=True):
        self.ledger = ledger
        self.state = state
        self._taa_acceptance_required = taa_acceptance_required

    @property
    def taa_acceptance_required(self):
        return self._taa_acceptance_required

    def reset(self):
        self.ledger.reset_uncommitted()
        if self.state:
            self.state.revertToHead(self.state.committedHeadHash)
