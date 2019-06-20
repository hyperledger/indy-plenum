import pytest

from plenum.common.constants import BLS_LABEL, TS_LABEL, IDR_CACHE_LABEL, ATTRIB_LABEL
from plenum.test.testing_utils import FakeSomething
from plenum.server.database_manager import DatabaseManager, Database
from common.exceptions import LogicError


@pytest.fixture(scope='function')
def database_manager():
    return DatabaseManager()


def test_register_database(database_manager: DatabaseManager):
    db_id = 1
    db_led = FakeSomething()
    db_state = FakeSomething()

    assert database_manager.get_database(db_id) is None
    assert database_manager.get_ledger(db_id) is None
    assert database_manager.get_state(db_id) is None

    database_manager.register_new_database(db_id, db_led, db_state)
    with pytest.raises(LogicError, match='Trying to add already existing database'):
        database_manager.register_new_database(db_id, FakeSomething(), FakeSomething())

    assert database_manager.get_database(db_id).ledger == db_led
    assert database_manager.get_database(db_id).state == db_state
    assert database_manager.get_ledger(db_id) == db_led
    assert database_manager.get_state(db_id) == db_state


def test_register_database_no_state(database_manager: DatabaseManager):
    db_id_1 = 1
    db_led_1 = FakeSomething()
    db_state_1 = FakeSomething()
    database_manager.register_new_database(db_id_1, db_led_1, db_state_1)

    assert db_id_1 in database_manager._ledgers
    assert db_id_1 in database_manager._states
    assert database_manager._ledgers[db_id_1] == db_led_1
    assert database_manager._states[db_id_1] == db_state_1

    db_id_2 = 2
    db_led_2 = FakeSomething()
    database_manager.register_new_database(db_id_2, db_led_2)

    assert db_id_2 in database_manager._ledgers
    assert db_id_2 not in database_manager._states
    assert database_manager._ledgers[db_id_2] == db_led_2


def test_register_store(database_manager: DatabaseManager):
    store_label = 'aaa'
    store = FakeSomething()

    assert database_manager.get_store(store_label) == None

    database_manager.register_new_store(store_label, store)

    assert database_manager.get_store(store_label) == store


def test_common_stores(database_manager: DatabaseManager):
    common_stores = [BLS_LABEL, TS_LABEL, IDR_CACHE_LABEL, ATTRIB_LABEL]

    assert database_manager.bls_store is None
    assert database_manager.ts_store is None
    assert database_manager.idr_cache is None
    assert database_manager.attribute_store is None

    for label in common_stores:
        database_manager.register_new_store(label, FakeSomething())

    assert database_manager.bls_store is not None
    assert database_manager.ts_store is not None
    assert database_manager.idr_cache is not None
    assert database_manager.attribute_store is not None


def test_database():
    led = FakeSomething()
    st = FakeSomething()
    db = Database(led, st)

    assert db.ledger == led
    assert db.state == st
