import pytest
from plenum.client.wallet import Wallet
from plenum.test.helper import TestRequestIdStore, randomSeed


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