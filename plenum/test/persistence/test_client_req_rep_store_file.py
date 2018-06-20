import pytest


from common.exceptions import ValueUndefinedError
from plenum.persistence.client_req_rep_store_file import ClientReqRepStoreFile

def test_ClientReqRepStoreFile_init_invalid_args():
    with pytest.raises(ValueUndefinedError):
        ClientReqRepStoreFile(None)
