import pytest
from plenum.client.request_id_store import FileRequestIdStore
import tempfile
import os

def test_file_request_id_store():
    # creating tem file
    import random
    storeFilePath = "{}/test_file_request_id_store_{}".format(tempfile.tempdir, random.random())
    with FileRequestIdStore(storeFilePath) as store:

        # since random empty file created for this test loaded storage should be empty
        assert len(store._storage) == 0

        clientId = "client-id"
        signerId = "signer-id"
        assert store.currentId(clientId, signerId) is None

        for i in range(5):
            reqId = store.nextId(clientId, signerId)
            assert reqId == i
            assert store.currentId(clientId, signerId) == reqId

    # check that store does contain the data
    assert os.path.getsize(storeFilePath) == 22