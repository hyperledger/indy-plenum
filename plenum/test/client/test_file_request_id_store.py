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


        for signerIndex in range(3):
            signerId = "signer-id-{}".format(signerIndex)
            assert store.currentId(signerId) is None
            for requestIndex in range(3):
                reqId = store.nextId(str(signerId))
                assert reqId == requestIndex + 1
                assert store.currentId(signerId) == reqId

    # check that store does contain the data
    assert os.path.getsize(storeFilePath) == 42
    os.remove(storeFilePath)