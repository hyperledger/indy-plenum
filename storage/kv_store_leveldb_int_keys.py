from storage.helper import integer_comparator
from storage.kv_store_leveldb import KeyValueStorageLeveldb

try:
    import leveldb
except ImportError:
    print('Cannot import leveldb, please install')


class KeyValueStorageLeveldbIntKeys(KeyValueStorageLeveldb):
    def __init__(self, db_dir, db_name, open=True, read_only=False):
        super().__init__(db_dir, db_name, open, read_only)

    def open(self):
        self._db = leveldb.LevelDB(self.db_path, comparator=(
            'IntegerComparator', integer_comparator))

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

    def get_last_key(self):
        last_key = None
        itr = self.iterator(include_value=False)
        if itr:
            all_keys = [_ for _ in itr]
            if len(all_keys) > 0:
                last_key = all_keys[-1]
        return last_key
