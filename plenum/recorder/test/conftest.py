import pytest

from plenum.recorder.src.recorder import Recorder
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys

from plenum.test.conftest import *

overriddenConfigValues['USE_RECORDER_WITH_STACK'] = True


@pytest.fixture()
def recorder(tmpdir_factory):
    storage = KeyValueStorageLeveldbIntKeys(
        tmpdir_factory.mktemp('').strpath, 'test_db')
    return Recorder(storage)
