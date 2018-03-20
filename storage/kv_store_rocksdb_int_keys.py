from storage.kv_store_rocksdb import KeyValueStorageRocksdb

try:
    import rocksdb
except ImportError:
    print('Cannot import rocksdb, please install')


class IntegerComparator(rocksdb.IComparator):
    def compare(self, a, b):
        a = int(a)
        b = int(b)
        if (a < b):
            return -1
        if (a > b):
            return 1
        return 0

    def name(self):
        return b'IntegerComparator'


class KeyValueStorageRocksdbIntKeys(KeyValueStorageRocksdb):
    def __init__(self, db_dir, db_name, open=True):
        super().__init__(db_dir, db_name, open)

    def open(self):
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.comparator = IntegerComparator()
        self._db = rocksdb.DB(self._db_path, opts)

    def get_equal_or_prev(self, key):
        # return value can be:
        #    None, if required key less then minimal key from DB
        #    Equal by key if key exist in DB
        #    Previous if key does not exist in Db, but there is key less than required

        if isinstance(key, int):
            key = str(key)
        if isinstance(key, str):
            key = key.encode()
        iter = self._db.itervalues()
        iter.seek_for_prev(key)
        try:
            value = next(iter)
        except StopIteration:
            value = None
        return value
