import os
import plyvel

from plenum.common.exceptions import StorageException
from plenum.common.log import getlogger

logger = getlogger()


class VerkeyStore:
    def __init__(self, basedir: str, name='verkey_store'):
        logger.debug('Initializing verkey {} store at {}'.format(name, basedir))
        self._basedir = basedir
        self._name = name
        self._db = None
        self.open()

    def get(self, did):
        self._checkDb()
        value = self._db.get(str.encode(did))
        if value:
            value = bytes.decode(value)
        return value

    def set(self, did, value):
        self._checkDb()
        self._db.put(str.encode(did), str.encode(value))

    def close(self):
        self._checkDb()
        self._db.close()
        self._db = None

    def open(self):
        self._db = plyvel.DB(self.dbName(), create_if_missing=True)

    def dbName(self):
        return os.path.join(self._basedir, self._name)

    def _checkDb(self):
        if not self._db:
            raise StorageException('Db reference is missing!')
        if self._db.closed:
            raise StorageException('Db is closed!')