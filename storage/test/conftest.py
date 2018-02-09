import pytest

from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store import KeyValueStorage
from storage.kv_store_leveldb import KeyValueStorageLeveldb


@pytest.fixture(scope='function')
def tempdir(tmpdir_factory):
    return tmpdir_factory.mktemp('').strpath


@pytest.yield_fixture(params=['memory', 'leveldb'])
def parametrised_storage(request, tmpdir_factory) -> KeyValueStorage:
    if request.param == 'memory':
        db = KeyValueStorageInMemory()
    elif request.param == 'leveldb':
        db = KeyValueStorageLeveldb(tmpdir_factory.mktemp('').strpath,
                                    'some_db')
    else:
        raise ValueError('Unsupported storage')
    yield db
    db.close()
