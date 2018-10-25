import pytest

from plenum.common.constants import KeyValueStorageType
from storage.helper import initKeyValueStorage
from storage.kv_store import KeyValueStorage

db_no = 0


@pytest.yield_fixture(params=[KeyValueStorageType.Rocksdb,
                              KeyValueStorageType.Leveldb,
                              KeyValueStorageType.BinaryFile])
def storage(request, tdir) -> KeyValueStorage:
    global db_no
    db = initKeyValueStorage(request.param, tdir, 'metrics_db_{}'.format(db_no))
    db_no += 1
    yield db
    db.close()
