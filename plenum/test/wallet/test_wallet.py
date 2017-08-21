import pytest
from plenum.client.wallet import Wallet
from plenum.common.util import getTimeBasedId
from stp_core.crypto.util import randomSeed


def add_and_sign(signersNum=10):
    beforeSignTimeBasedId = getTimeBasedId()
    wallet = Wallet("shared wallet")
    idrs = []
    for i in range(signersNum):
        identifier = "signer_{}".format(i)
        idrs.append(identifier)
        wallet.addIdentifier(identifier)
    for idr in idrs:
        signed = wallet.signOp(op={}, identifier=idr)
        afterSignTimeBasedId = getTimeBasedId()
        assert beforeSignTimeBasedId < signed.reqId < afterSignTimeBasedId


def test_wallet_uses_store():
    add_and_sign(signersNum=1)


def test_wallet_used_with_multiple_signers():
    add_and_sign(signersNum=10)
