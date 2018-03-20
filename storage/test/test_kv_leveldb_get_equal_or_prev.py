import pytest
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys
from storage.kv_store_rocksdb_int_keys import KeyValueStorageRocksdbIntKeys


@pytest.fixture(scope="module", params=['rocksdb', 'leveldb'])
def storage_with_ts_root_hashes(request, tmpdir_factory):
    if request.param == 'leveldb':
        storage = KeyValueStorageLeveldbIntKeys(tmpdir_factory.mktemp('').strpath,
                                                "test_db")
    else:
        storage = KeyValueStorageRocksdbIntKeys(tmpdir_factory.mktemp('').strpath,
                                                "test_db")
    ts_list = {
        2: "aaaa",
        4: "bbbb",
        5: "cccc",
        100: "ffff",
    }
    for k, v in ts_list.items():
        storage.put(str(k), v)
    return storage, ts_list


def test_none_if_key_less_then_minimal_key(storage_with_ts_root_hashes):
    storage, _ = storage_with_ts_root_hashes
    assert storage.get_equal_or_prev(1) is None


def test_previous_key_for_given(storage_with_ts_root_hashes):
    storage, ts_list = storage_with_ts_root_hashes
    assert storage.get_equal_or_prev(3).decode("utf-8") == ts_list[2]
    assert storage.get_equal_or_prev(101).decode("utf-8") == ts_list[100]


def test_get_required_key(storage_with_ts_root_hashes):
    storage, ts_list = storage_with_ts_root_hashes
    assert storage.get_equal_or_prev(2).decode("utf-8") == ts_list[2]
