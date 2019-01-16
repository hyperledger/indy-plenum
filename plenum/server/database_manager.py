from typing import Dict

from common.exceptions import LogicError
from plenum.common.constants import BLS_LABEL, TS_LABEL, IDR_CACHE_LABEL, ATTRIB_LABEL
from plenum.common.ledger import Ledger
from state.state import State


class DatabaseManager():
    def __init__(self):
        self.__databases = {}  # type: Dict[int, Database]
        self.__stores = {}
        self.__states = {}

    def register_database(self, lid, ledger: Ledger, state: State):
        if lid in self.__databases:
            raise LogicError('Trying to add already existing database')
        self.__databases[lid] = Database(ledger, state)
        self.__states[lid] = state

    def flush_database(self, lid):
        del self.__databases[lid]
        del self.__states[lid]

    def get_database(self, lid):
        if lid not in self.__databases:
            return None
        return self.__databases[lid]

    def register_store(self, label, store):
        if label in self.__stores:
            raise LogicError('Trying to add already existing store')
        self.__stores[label] = store

    def get_store(self, label):
        if label not in self.__stores:
            return None
        return self.__stores[label]

    @property
    def states(self):
        # TODO: change this. Too inefficient to build dict every time
        return dict((lid, db.state) for lid, db in self.__databases.items())

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


class Database:
    def __init__(self, ledger, state):
        self.ledger = ledger
        self.state = state
