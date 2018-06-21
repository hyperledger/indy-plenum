import pytest

from state.db.refcount_db import RefcountDB
from storage.kv_in_memory import KeyValueStorageInMemory


def test_dec_refcount():
    db = RefcountDB(KeyValueStorageInMemory())
    k = b'123'

    db.inc_refcount(k, '')

    db.dec_refcount(k)
    with pytest.raises(ValueError) as excinfo:
        db.dec_refcount(k)
    assert ("node object for key {} has "
            "0 number of references".format(k)) in str(excinfo.value)
