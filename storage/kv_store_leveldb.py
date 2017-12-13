import os
import shutil
from typing import Iterable, Tuple

from state.util.utils import removeLockFiles
from storage.kv_store import KeyValueStorage

try:
    import leveldb
except ImportError:
    print('Cannot import leveldb, please install')


class KeyValueStorageLeveldb(KeyValueStorage):
    def __init__(self, db_dir, db_name, open=True):
        if 'leveldb' not in globals():
            raise RuntimeError('Leveldb is needed to use this class')
        self.db_path = os.path.join(db_dir, db_name)
        self._db = None
        if open:
            self.open()

    def __repr__(self):
        return self.db_path

    @property
    def is_byte(self) -> bool:
        return True

    def db_path(self) -> str:
        return self.db_path

    def iterator(self, start=None, end=None, include_key=True, include_value=True, prefix=None):
        if start and isinstance(start, int):
            start = str(start)
        if end and isinstance(end, int):
            end = str(end)
        if start and isinstance(start, str):
            start = start.encode()
        if end and isinstance(end, str):
            end = end.encode()

        return self._db.RangeIter(key_from=start, key_to=end, include_value=include_value)

    def put(self, key, value):
        if isinstance(key, str):
            key = key.encode()
        if isinstance(value, str):
            value = value.encode()
        self._db.Put(key, value)

    def get(self, key):
        if isinstance(key, str):
            key = key.encode()
        return self._db.Get(key)

    def remove(self, key):
        if isinstance(key, str):
            key = key.encode()
        self._db.Delete(key)

    def setBatch(self, batch: Iterable[Tuple]):
        b = leveldb.WriteBatch()
        for key, value in batch:
            if isinstance(key, str):
                key = key.encode()
            if isinstance(value, str):
                value = value.encode()
            b.Put(key, value)
        self._db.Write(b, sync=False)

    def do_ops_in_batch(self, batch: Iterable[Tuple]):
        b = leveldb.WriteBatch()
        for op, key, value in batch:
            if isinstance(key, str):
                key = key.encode()
            if isinstance(value, str):
                value = value.encode()
            if op == self.WRITE_OP:
                b.Put(key, value)
            elif op == self.REMOVE_OP:
                b.Delete(key)
            else:
                raise ValueError('Unknown operation')
        self._db.Write(b, sync=False)

    def open(self):
        self._db = leveldb.LevelDB(self.db_path)

    def close(self):
        removeLockFiles(self.db_path)
        del self._db
        self._db = None

    def drop(self):
        self.close()
        shutil.rmtree(self.db_path)

    def reset(self):
        self.drop()
        self.open()

    @property
    def closed(self):
        return self._db is None
