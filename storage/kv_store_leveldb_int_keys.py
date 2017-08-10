import os
import shutil
from typing import Iterable, Tuple

from state.util.utils import removeLockFiles
from storage.kv_store import KeyValueStorage
from storage.kv_store_leveldb import KeyValueStorageLeveldb

try:
    import leveldb
except ImportError:
    print('Cannot import leveldb, please install')

def compare(a, b):
    a = int(a)
    b = int(b)
    if (a < b):
        return -1
    if (a > b):
        return 1
    return 0

class KeyValueStorageLeveldbIntKeys(KeyValueStorageLeveldb):

    def __init__(self, db_dir, db_name, open=True):
        super().__init__(db_dir, db_name, open)

    def open(self):
        self._db = leveldb.LevelDB(self.db_path, comparator=('IntegerComparator', compare))
