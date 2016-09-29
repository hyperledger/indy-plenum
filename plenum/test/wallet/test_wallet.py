import pytest
from plenum.client.wallet import Wallet
from plenum.persistence.client_req_rep_store import ClientReqRepStore

class TestClientReqRepStore:
    def __init__(self, lastReqId):
        self._lastReqId = lastReqId
    @property
    def lastReqId(self):
        return self._lastReqId

def test_wallet_uses_store():
    lastReqId = 128
    store = TestClientReqRepStore(lastReqId) #type: ClientReqRepStore

    wallet = Wallet("client_name")
    identifier = "signer_identifier"
    seed = str.encode("x"*32)

    wallet.addSigner(identifier, seed, reqRepStore=store)

    op = {}

    signed = wallet.signOp(op)
    assert signed.reqId == lastReqId + 1