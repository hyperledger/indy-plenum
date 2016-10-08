import pytest
from plenum.client.request_id_store import *
import os
from plenum.test.conftest import tdir
import random


def check_request_id_store(store: RequestIdStore):
    for signerIndex in range(3):
        signerId = "signer-id-{}".format(signerIndex)
        assert store.currentId(signerId) is None
        for requestIndex in range(3):
            reqId = store.nextId(str(signerId))
            assert reqId == requestIndex + 1
            assert store.currentId(signerId) == reqId


@pytest.mark.skipif(True, reason="Request id generated on the basis of time")
def test_file_request_id_store(tdir):
    # creating tem file
    os.mkdir(tdir)
    storeFileName = "test_file_request_id_store_{}".format(random.random())
    storeFilePath = os.path.join(tdir, storeFileName)
    with FileRequestIdStore(storeFilePath) as store:
        # since random empty file created for this test
        # loaded storage should be empty
        assert len(store._storage) == 0
        # TODO: For now, we are using random number for req id, so the
        # present check won't work, probably can look it later
        # check_request_id_store(store)
    # check that store does contain the data
    assert os.path.getsize(storeFilePath) == 42
    os.remove(storeFilePath)


@pytest.mark.skipif(True, reason="Request id generated on the basis of time")
def test_memory_request_id_store():
    store = MemoryRequestIdStore()
    check_request_id_store(store)