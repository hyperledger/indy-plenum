import pytest
from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.kv_store_rocksdb import KeyValueStorageRocksdb
from storage.kv_in_memory import KeyValueStorageInMemory
from storage.kv_store import KeyValueStorage

i = 0


@pytest.yield_fixture(scope="function", params=['rocksdb', 'leveldb', 'in_memory'])
def kv(request, tempdir) -> KeyValueStorage:
    global i

    if request.param == 'leveldb':
        kv = KeyValueStorageLeveldb(tempdir, 'kv{}'.format(i))
    elif request.param == 'rocksdb':
        kv = KeyValueStorageRocksdb(tempdir, 'kv{}'.format(i))
    else:
        kv = KeyValueStorageInMemory()

    i += 1
    yield kv
    kv.close()


def test_reopen(kv):
    kv.put('k1', 'v1')
    v1 = kv.get('k1')
    kv.close()

    kv.open()
    v2 = kv.get('k1')

    assert b'v1' == v1
    assert b'v1' == v2


def test_drop(kv):
    kv.put('k1', 'v1')
    hasKeyBeforeDrop = 'k1' in kv
    kv.close()
    kv.drop()

    kv.open()
    hasKeyAfterDrop = 'k1' in kv

    assert hasKeyBeforeDrop
    assert not hasKeyAfterDrop


def test_put_none(kv):
    if isinstance(kv, KeyValueStorageInMemory):
        kv.put('k1', None)
    else:
        pass


def test_put_string(kv):
    kv.put('k1', 'v1')
    v1 = kv.get('k1')

    kv.put('k2', 'v2')
    v2 = kv.get('k2')

    kv.put('k1', 'v3')
    v3 = kv.get('k1')
    v4 = kv.get('k2')

    assert b'v1' == v1
    assert b'v2' == v2
    assert b'v3' == v3
    assert b'v2' == v4


def test_put_bytes(kv):
    kv.put(b'k1', b'v1')
    v1 = kv.get(b'k1')

    kv.put(b'k2', b'v2')
    v2 = kv.get(b'k2')

    kv.put(b'k1', b'v3')
    v3 = kv.get(b'k1')
    v4 = kv.get(b'k2')

    assert b'v1' == v1
    assert b'v2' == v2
    assert b'v3' == v3
    assert b'v2' == v4


def test_put_string_and_bytes(kv):
    kv.put(b'k1', 'v1')
    v1 = kv.get('k1')

    kv.put('k2', b'v2')
    v2 = kv.get(b'k2')

    kv.put('k1', b'v3')
    v3 = kv.get('k1')
    v4 = kv.get('k2')

    assert b'v1' == v1
    assert b'v2' == v2
    assert b'v3' == v3
    assert b'v2' == v4


def test_remove_string(kv):
    kv.put('k1', 'v1')
    hasKeyBeforeRemove = 'k1' in kv
    kv.remove('k1')
    hasKeyAfterRemove = 'k1' in kv

    assert hasKeyBeforeRemove
    assert not hasKeyAfterRemove


def test_remove_bytes(kv):
    kv.put(b'k1', b'v1')
    hasKeyBeforeRemove = b'k1' in kv
    kv.remove(b'k1')
    hasKeyAfterRemove = b'k1' in kv

    assert hasKeyBeforeRemove
    assert not hasKeyAfterRemove


def test_batch_string(kv):
    batch = [('k'.format(i), 'v'.format(i))
             for i in range(5)]
    kv.setBatch(batch)

    for i in range(5):
        assert 'v'.format(i).encode() == kv.get('k'.format(i))


def test_batch_bytes(kv):
    batch = [('k'.format(i).encode(), 'v'.format(i).encode())
             for i in range(5)]
    kv.setBatch(batch)

    for i in range(5):
        assert 'v'.format(i).encode() == kv.get('k'.format(i))
