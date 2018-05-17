import os

from typing import Iterable, Tuple

import shutil
from storage.kv_store import KeyValueStorage
from state.util.utils import removeLockFiles

try:
    import rocksdb
except ImportError:
    print('Cannot import rocksdb, please install')


class KeyValueStorageRocksdb(KeyValueStorage):
    def __init__(self, db_dir, db_name, open=True, read_only=False):
        if 'rocksdb' not in globals():
            raise RuntimeError('Rocksdb is needed to use this class')
        self._db_path = os.path.join(db_dir, db_name)
        self._read_only = read_only
        self._db = None
        if open:
            self.open()

    def open(self):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        self._db = rocksdb.DB(self._db_path, opts, read_only=self._read_only)

    def __repr__(self):
        return self._db_path

    @property
    def is_byte(self) -> bool:
        return True

    @property
    def db_path(self) -> str:
        return self._db_path

    @property
    def read_only(self) -> bool:
        return self._read_only

    @property
    def closed(self):
        return self._db is None

    def put(self, key, value):
        key = self.to_byte_repr(key)
        value = self.to_byte_repr(value)
        self._db.put(key, value)

    def get(self, key):
        key = self.to_byte_repr(key)
        vv = self._db.get(key)
        if vv is None:
            raise KeyError
        return vv

    def remove(self, key):
        key = self.to_byte_repr(key)
        self._db.delete(key)

    def setBatch(self, batch: Iterable[Tuple]):
        b = rocksdb.WriteBatch()
        for key, value in batch:
            key = self.to_byte_repr(key)
            value = self.to_byte_repr(value)
            b.put(key, value)
        self._db.write(b, sync=False)

    def close(self):
        del self._db
        self._db = None
        removeLockFiles(self._db_path)

    def drop(self):
        self.close()
        shutil.rmtree(self._db_path)

    def reset(self):
        self.drop()
        self.open()

    def iterator(self, start=None, end=None, include_key=True, include_value=True, prefix=None):
        start = self.to_byte_repr(start) if start is not None else None
        end = self.to_byte_repr(end) if end is not None else None

        #  TODO: Figure out why this does not work
        # opts = {}
        # if start:
        #     opts['iterate_lower_bound'] = start
        # if end:
        #     opts['iterate_upper_bound'] = end
        # if not include_value:
        #     itr = self._db.iterkeys(opts)
        # else:
        #     itr = self._db.iteritems(opts)

        if not include_value:
            itr = self._db.iterkeys()
        else:
            itr = self._db.iteritems()

        if start:
            itr.seek(start)
        else:
            itr.seek_to_first()

        if end:
            itr = self._new_wrapped_iterator(itr, end)

        return itr

    def do_ops_in_batch(self, batch: Iterable[Tuple], is_committed=False):
        pass

    def has_key(self, key):
        key = self.to_byte_repr(key)
        return self._db.key_may_exist(key)[0]

    @staticmethod
    def _new_wrapped_iterator(itr, upper_bound):
        # Takes Rocksdb iterator and an upper bound and returns another
        # iterator which goes till the upper bound (inclusive)
        return WrappingIter(itr, upper_bound)


class WrappingIter:
    def __init__(self, iterator, upper_bound):
        self.iterator = iterator
        self.upper_bound = upper_bound
        self.reached_end = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.reached_end is True:
            raise StopIteration
        item = next(self.iterator)
        key = item[0] if isinstance(item, tuple) else item
        if key == self.upper_bound:
            self.reached_end = True
        return item
