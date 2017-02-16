import os
import leveldb

from plenum.common.exceptions import StorageException
from plenum.common.log import getlogger

logger = getlogger()


class VerkeyStore:
    guardianPrefix = b'\xf0\x9f\x98\x80'

    def __init__(self, basedir: str, name='verkey_store'):
        logger.debug('Initializing verkey {} store at {}'.format(name, basedir))
        self._basedir = basedir
        self._name = name
        self._db = None
        self.open()

    def get(self, did, unpack=False):
        self._checkDb()
        did = str.encode(did)
        value = self._db.Get(did)
        if value:
            if unpack and (value[:len(VerkeyStore.guardianPrefix)] == VerkeyStore.guardianPrefix):
                return self.get(bytes.decode(value[len(VerkeyStore.guardianPrefix):]))
            value = bytes.decode(value)
        return value

    def set(self, did, value, guarded=False):
        self._checkDb()
        value = str.encode(value)
        did = str.encode(did)
        if guarded:
            value = VerkeyStore.guardianPrefix + value
        self._db.Put(did, value)

    def close(self):
        self._db = None

    def open(self):
        self._db = leveldb.LevelDB(self.dbName())

    def dbName(self):
        return os.path.join(self._basedir, self._name)

    def _checkDb(self):
        if not self._db:
            raise StorageException('Db reference is missing!')

    @property
    def guardianPrefixDecoded(self):
        return bytes.decode(VerkeyStore.guardianPrefix)