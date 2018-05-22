import pytest

from plenum.recorder.src.recorder import Recorder
from plenum.recorder.test.helper import reload_modules_for_recorder
from plenum.test.conftest import _tconf
from storage.kv_store_leveldb_int_keys import KeyValueStorageLeveldbIntKeys

from plenum.test.conftest import *

# overriddenConfigValues['USE_WITH_STACK'] = 1
#
#
# @pytest.fixture(scope="module")
# def tconf(general_conf_tdir):
#     conf = _tconf(general_conf_tdir)
#     reload_modules_for_recorder(conf)
#     return conf


@pytest.fixture()
def recorder(tmpdir_factory):
    storage = KeyValueStorageLeveldbIntKeys(
        tmpdir_factory.mktemp('').strpath, 'test_db')
    return Recorder(storage)

