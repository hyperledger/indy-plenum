import pytest
import random
from plenum.client.wallet import Wallet
from plenum.persistence.client_req_rep_store import ClientReqRepStore

class TestClientReqRepStore:
    def __init__(self, lastReqId):
        self._lastReqId = lastReqId - 1
    @property
    def lastReqId(self):
        self._lastReqId += 1
        return self._lastReqId

def add_and_sign(clientsNum = 10):
    # configuring wallet
    wallet = Wallet("shared wallet")
    for i in range(clientsNum):
        store = TestClientReqRepStore(i)  # type: ClientReqRepStore
        identifier = "signer_{}".format(i)
        seed = str.encode("".join(random.choice("0123456789abcdef") for _ in range(32)))
        wallet.addSigner(identifier, seed, reqRepStore=store)

    # testing indexes
    for i in range(clientsNum):
        op = {}
        signed = wallet.signOp(op)
        assert i + 1 == signed.reqId

def test_wallet_uses_store():
    add_and_sign(clientsNum=1)

def test_wallet_used_with_multiple_signers():
    add_and_sign(clientsNum=10)