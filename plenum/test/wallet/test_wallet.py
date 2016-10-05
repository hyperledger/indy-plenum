import pytest
import random
from plenum.client.wallet import Wallet
from plenum.client.request_id_store import RequestIdStore

class TestRequestIdStore(RequestIdStore):

    def __init__(self):
        self._currentId = -1

    def nextId(self, signerId) -> int:
        self._currentId += 1
        return self._currentId

    def currentId(self, signerId) -> int:
        return self._currentId

def randomSeed():
    chars = "0123456789abcdef"
    return str.encode("".join(random.choice(chars) for _ in range(32)))

def add_and_sign(signersNum = 10):
    store = TestRequestIdStore()
    wallet = Wallet("shared wallet", store)
    idrs = []
    for i in range(signersNum):
        identifier = "signer_{}".format(i)
        idrs.append(identifier)
        wallet.addSigner(identifier, seed=randomSeed())
    for idr in idrs:
        signed = wallet.signOp(op = {}, identifier=idr)
        assert signed.reqId == store.currentId(idr)

def test_wallet_uses_store():
    add_and_sign(signersNum=1)

def test_wallet_used_with_multiple_signers():
    add_and_sign(signersNum=10)