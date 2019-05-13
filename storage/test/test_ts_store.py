import pytest
from storage.helper import initKeyValueStorageIntKeys
from plenum.common.constants import KeyValueStorageType, DOMAIN_LEDGER_ID, CONFIG_LEDGER_ID
from storage.state_ts_store import StateTsDbStorage


@pytest.fixture(scope="function", params=['rocksdb', 'leveldb'])
def empty_storage(request, tmpdir_factory):
    if request.param == 'leveldb':
        kv_storage_type = KeyValueStorageType.Leveldb
    else:
        kv_storage_type = KeyValueStorageType.Rocksdb

    data_location = tmpdir_factory.mktemp('').strpath

    domain_storage = initKeyValueStorageIntKeys(
        kv_storage_type,
        data_location,
        "test_db")

    config_storage = initKeyValueStorageIntKeys(
        kv_storage_type,
        data_location,
        "config_test_db")

    storage = StateTsDbStorage("test", {
        DOMAIN_LEDGER_ID: domain_storage,
        CONFIG_LEDGER_ID: config_storage
    })

    return storage


@pytest.fixture(scope="function")
def storage_with_ts_root_hashes(empty_storage):
    storage = empty_storage

    domain_ts_list = {
        2: "aaaa",
        4: "bbbb",
        5: "cccc",
        100: "ffff"
    }

    config_ts_list = {
        0: "pppp",
        3: "qqqq",
        5: "rrrr",
        50: "ssss"
    }

    for k, v in domain_ts_list.items():
        storage.set(k, v)
    for k, v in config_ts_list.items():
        storage.set(k, v, ledger_id=CONFIG_LEDGER_ID)

    return storage, domain_ts_list, config_ts_list


def test_none_if_key_less_then_minimal_key(storage_with_ts_root_hashes):
    storage, domain_ts_list, config_ts_list = storage_with_ts_root_hashes
    min_domain_ts = min(domain_ts_list.keys())
    min_config_ts = min(config_ts_list.keys())
    assert storage.get_equal_or_prev(min_domain_ts - 1) is None
    assert storage.get_equal_or_prev(min_config_ts - 1, DOMAIN_LEDGER_ID) is None
    assert storage.get_equal_or_prev(min_config_ts - 1, CONFIG_LEDGER_ID) is None


def test_previous_key_for_given(storage_with_ts_root_hashes):
    storage, ts_list, _ = storage_with_ts_root_hashes
    storage, domain_ts_list, config_ts_list = storage_with_ts_root_hashes
    domain_ts_list = {ts: root_hash for ts, root_hash in domain_ts_list.items() if ts + 1 not in domain_ts_list}
    config_ts_list = {ts: root_hash for ts, root_hash in config_ts_list.items() if ts + 1 not in config_ts_list}
    for ts, root_hash in domain_ts_list.items():
        assert storage.get_equal_or_prev(ts + 1).decode() == root_hash
        assert storage.get_equal_or_prev(ts + 1, DOMAIN_LEDGER_ID).decode() == root_hash
    for ts, root_hash in config_ts_list.items():
        assert storage.get_equal_or_prev(ts + 1, CONFIG_LEDGER_ID).decode() == root_hash


def test_get_required_key(storage_with_ts_root_hashes):
    storage, domain_ts_list, config_ts_list = storage_with_ts_root_hashes
    for ts, root_hash in domain_ts_list.items():
        assert storage.get_equal_or_prev(ts).decode() == root_hash
        assert storage.get_equal_or_prev(ts, DOMAIN_LEDGER_ID).decode() == root_hash
    for ts, root_hash in config_ts_list.items():
        assert storage.get_equal_or_prev(ts, CONFIG_LEDGER_ID).decode() == root_hash


def test_get_last_key(storage_with_ts_root_hashes):
    storage, domain_ts_list, config_ts_list = storage_with_ts_root_hashes
    assert storage.get_last_key().decode() == str(max(domain_ts_list.keys()))
    assert storage.get_last_key(DOMAIN_LEDGER_ID).decode() == str(max(domain_ts_list.keys()))
    assert storage.get_last_key(CONFIG_LEDGER_ID).decode() == str(max(config_ts_list.keys()))


def test_empty_storage_get_last_key(empty_storage):
    storage = empty_storage
    assert storage.get_last_key() is None
    assert storage.get_last_key(CONFIG_LEDGER_ID) is None
