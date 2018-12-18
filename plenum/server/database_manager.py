from typing import Dict

from common.exceptions import LogicError
from plenum.common.ledger import Ledger
from state.state import State


class DatabaseManager():
    def __init__(self):
        self.databases = {}  # type: Dict[int, Database]
        self.stores = {}

    def register_new_database(self, lid, ledger: Ledger, state: State):
        if lid in self.databases:
            raise LogicError('Trying to add already existing database')
        self.databases[lid] = Database(ledger, state)

    def get_database(self, lid):
        if lid not in self.databases:
            raise LogicError('Trying to get nonexistent database')
        return self.databases[lid]

    def register_new_store(self, label, store):
        self.stores[label] = store

    def get_store(self, label):
        if label not in self.stores:
            raise LogicError('Trying to get nonexistent store')
        return self.stores[label]


class Database:
    def __init__(self, ledger, state):
        self.ledger = ledger
        self.state = state
