import pytest
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.kv_store_rocksdb import KeyValueStorageRocksdb
from storage.kv_store import KeyValueStorage

i = 0


@pytest.yield_fixture(scope="function", params=['rocksdb', 'leveldb'])
def kv(request, tempdir) -> KeyValueStorage:
    global i

    if request.param == 'leveldb':
        kv = KeyValueStorageLeveldb(tempdir, 'kv{}'.format(i))
    else:
        kv = KeyValueStorageRocksdb(tempdir, 'kv{}'.format(i))

    assert kv.read_only is False

    kv.put('k1', 'v1')
    kv.put('k2', 'v2')
    kv.put('k3', 'v3')
    kv.close()

    if request.param == 'leveldb':
        kv = KeyValueStorageLeveldb(tempdir, 'kv{}'.format(i), read_only=True)
    else:
        kv = KeyValueStorageRocksdb(tempdir, 'kv{}'.format(i), read_only=True)

    i += 1
    yield kv
    kv.close()


def test_read_only_get(kv):
    assert kv.read_only is True

    v = kv.get('k1')
    assert b'v1' == v

    v = kv.get('k2')
    assert b'v2' == v

    v = kv.get('k3')
    assert b'v3' == v


def test_read_only_put(kv):
    assert kv.read_only is True

    with pytest.raises(Exception, match="Not supported operation in read only mode."):
        kv.put('k4', 'v4')


def test_read_only_remove(kv):
    assert kv.read_only is True

    with pytest.raises(Exception, match="Not supported operation in read only mode."):
        kv.remove('k1')
