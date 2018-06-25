from storage.helper import integer_comparator
from storage.kv_store_rocksdb import KeyValueStorageRocksdb

try:
    import rocksdb
except ImportError:
    print('Cannot import rocksdb, please install')


class IntegerComparator(rocksdb.IComparator):
    def compare(self, a, b):
        return integer_comparator(a, b)

    def name(self):
        return b'IntegerComparator'


class KeyValueStorageRocksdbIntKeys(KeyValueStorageRocksdb):
    def __init__(self, db_dir, db_name, open=True, read_only=False, db_config=None):
        super().__init__(db_dir, db_name, open, read_only, db_config)
        self._read_only = read_only

    def open(self):
        opts = self._get_db_opts()
        opts.comparator = IntegerComparator()
        self._db = rocksdb.DB(self._db_path, opts, read_only=self._read_only)

    def get_equal_or_prev(self, key):
        # return value can be:
        #    None, if required key less then minimal key from DB
        #    Equal by key if key exist in DB
        #    Previous if key does not exist in Db, but there is key less than required

        key = self.to_byte_repr(key)
        itr = self._db.itervalues()
        itr.seek_for_prev(key)
        try:
            value = next(itr)
        except StopIteration:
            value = None
        return value

    def get_last_key(self):
        itr = self._db.iterkeys()
        itr.seek_to_last()
        try:
            key = next(itr)
        except StopIteration:
            key = None
        return key
