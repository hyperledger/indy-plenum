import pytest
from storage.helper import initKeyValueStorageIntKeys
from plenum.common.constants import KeyValueStorageType
from storage.state_ts_store import StateTsDbStorage


@pytest.fixture(scope="function", params=['rocksdb', 'leveldb'])
def empty_storage(request, tmpdir_factory):
    if request.param == 'leveldb':
        kv_storage_type = KeyValueStorageType.Leveldb
    else:
        kv_storage_type = KeyValueStorageType.Rocksdb

    storage = StateTsDbStorage("test",
                               initKeyValueStorageIntKeys(
                                   kv_storage_type,
                                   tmpdir_factory.mktemp('').strpath,
                                   "test_db"))
    return storage


@pytest.fixture(scope="function")
def storage_with_ts_root_hashes(empty_storage):
    storage = empty_storage

    ts_list = {
        2: "aaaa",
        4: "bbbb",
        5: "cccc",
        100: "ffff",
    }
    for k, v in ts_list.items():
        storage.set(k, v)
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


def test_get_last_key(storage_with_ts_root_hashes):
    storage, ts_list = storage_with_ts_root_hashes
    assert storage.get_last_key().decode() == '100'


def test_empty_storage_get_last_key(empty_storage):
    storage = empty_storage
    assert storage.get_last_key() is None
