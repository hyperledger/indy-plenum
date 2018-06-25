import random

import pytest

from storage.kv_store_leveldb import KeyValueStorageLeveldb
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from storage.kv_store_rocksdb import KeyValueStorageRocksdb
from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys

db_no = 0


@pytest.yield_fixture(params=['rocksdb', 'leveldb'])
def db_with_int_comparator(request, tempdir) -> KeyValueStorageLeveldb:
    global db_no
    if request.param == 'leveldb':
        db = KeyValueStorageLeveldbIntKeys(tempdir, 'kv{}'.format(db_no))
    else:
        db = KeyValueStorageRocksdbIntKeys(tempdir, 'kv{}'.format(db_no))
    db_no += 1
    yield db
    db.close()


@pytest.yield_fixture(params=['rocksdb', 'leveldb'])
def db_with_no_comparator(request, tempdir) -> KeyValueStorageLeveldb:
    global db_no
    if request.param == 'leveldb':
        db = KeyValueStorageLeveldb(tempdir, 'kv{}'.format(db_no))
    else:
        db = KeyValueStorageRocksdb(tempdir, 'kv{}'.format(db_no))
    db_no += 1
    yield db
    db.close()


def test_get_keys_in_order(db_with_int_comparator):
    # Add keys in arbitrary order but retrieve in sorted order
    db = db_with_int_comparator
    db.put('50', 'a')
    db.put('1', 'b')
    db.put('100', 'c')
    db.put('2', 'd')
    db.put('200', 'e')
    db.put('801', 'f')
    db.put('9', 'g')
    db.put('301', 'h')
    db.put('-100', 'i')
    db.put('-5', 'j')
    db.put('-26', 'k')

    ordered = [('-100', 'i'), ('-26', 'k'), ('-5', 'j'), ('1', 'b'), ('2', 'd'),
               ('9', 'g'), ('50', 'a'), ('100', 'c'), ('200', 'e'),
               ('301', 'h'), ('801', 'f')]
    i = 0
    for k, v in db.iterator():
        assert (k, v) == (ordered[i][0].encode(), ordered[i][1].encode())
        i += 1

    # add random keys and check retrieval happens in sorted order
    for _ in range(100):
        k = str(random.randint(100, 1000000000))
        db.put(k, str(k + '00'))

    all_keys = [int(k) for k, _ in db.iterator()]
    assert all_keys == sorted(all_keys)


def test_get_keys_from_a_given_key(db_with_int_comparator):
    # Retrieve keys starting from an arbitrary key, or ending at an
    # arbitrary key
    db = db_with_int_comparator
    all_keys = []
    for _ in range(1000):
        _k = _ + 100000000
        k = str(_k)
        db.put(k, str(k + '00'))
        all_keys.append(_k)

    # Needed for test below
    all_keys = sorted(all_keys)
    for _ in range(100):
        # For a random key, check that keys retrived from db are in sorted order
        random_key = random.choice(all_keys)
        random_key_index = all_keys.index(random_key)
        i = 0
        for k, _ in db.iterator(start=random_key):
            assert all_keys[random_key_index + i] == int(k)
            i += 1

        # all keys are retrieved
        assert all_keys[-1] == int(k)

    start, end = 200, 500
    i = 0
    for k, _ in db.iterator(start=all_keys[start], end=all_keys[end]):
        assert all_keys[start + i] == int(k)
        i += 1

    # last key retrieved was all_keys[end]
    assert all_keys[end] == int(k)


def test_get_keys_with_prefix(db_with_no_comparator):
    # Get keys with a certain prefix
    db = db_with_no_comparator

    prefixes = [1, 2, 5, 100, 300, 80, 46, 992]
    keys_by_pfx = {}
    all_keys = []
    for p in prefixes:
        keys_by_pfx[p] = set()
        for i in range(5):
            k = '{}:{}'.format(p, random.randint(1, 1000))
            keys_by_pfx[p].add(k)
            all_keys.append(k)

    # Insert keys in random order
    random.shuffle(all_keys)
    for k in all_keys:
        db.put(k, str(random.random()))

    for p in prefixes:
        retrieved_keys = set()
        pfx = '{}:'.format(p)
        for k, _ in db.iterator(start=pfx):
            k = k.decode("utf-8")
            if k.startswith(pfx):
                retrieved_keys.add(k)
            else:
                break

        # All and only keys with the prefix are retrieved
        assert retrieved_keys == set(keys_by_pfx[p])


def test_large_integer_keys(db_with_int_comparator):
    # Retrieve keys starting from an arbitrary key, or ending at an
    # arbitrary key
    db = db_with_int_comparator
    k1, k2 = b'308085874513273', b'308086276986684'
    db.put(k1, '1')
    db.put(k2, '2')
    assert db.get(k1) == bytearray(b'1')
    assert db.get(k2) == bytearray(b'2')

    k00 = b'2147483617'
    db.put(k00, '00')

    k00 = b'2147483648'
    db.put(k00, '00')

    k0 = b'4294967298'
    db.put(k0, '0')

    k3 = b'1157920892373161954235709850086879078532699846656405640394575840079131296398450'
    k4 = b'2157920892373161954235709850086879078532699846656405640394575840079131296398550'

    db.put(k3, '1')
    db.put(k4, '2')
    assert db.get(k3) == bytearray(b'1')
    assert db.get(k4) == bytearray(b'2')
