from storage.kv_store_leveldb import KeyValueStorageLeveldb

try:
    import leveldb
except ImportError:
    print('Cannot import leveldb, please install')


class KeyValueStorageLeveldbIntKeys(KeyValueStorageLeveldb):
    def __init__(self, db_dir, db_name, open=True):
        super().__init__(db_dir, db_name, open)

    @staticmethod
    def compare(a, b):
        a = int(a)
        b = int(b)
        if (a < b):
            return -1
        if (a > b):
            return 1
        return 0

    def open(self):
        self._db = leveldb.LevelDB(self.db_path, comparator=(
            'IntegerComparator', self.compare))

    def get_equal_or_prev(self, key):
        # return value can be:
        #    None, if required key less then minimal key from DB
        #    Equal by key if key exist in DB
        #    Previous if key does not exist in Db, but there is key less than required

        if isinstance(key, str):
            key = int(key)
        try:
            value = self.get(str(key))
        except KeyError:
            prev_value = None
            for k, v in self.iterator():
                if int(k) > key:
                    break
                prev_value = v
            value = prev_value
        return value
