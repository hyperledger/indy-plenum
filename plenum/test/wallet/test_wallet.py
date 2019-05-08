import pytest
from plenum.client.wallet import Wallet
from plenum.common.constants import CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import InsufficientSignatures
from plenum.common.request import Request
from plenum.common.util import getTimeBasedId
from plenum.server.client_authn import CoreAuthNr
from plenum.test.helper import randomOperation
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


def test_wallet_multisig():
    wallet = Wallet()
    idr1, signer1 = wallet.addIdentifier()
    idr2, signer2 = wallet.addIdentifier()
    idr3, signer3 = wallet.addIdentifier()
    authnr = CoreAuthNr()
    for idr, signer in [(idr1, signer1), (idr2, signer2), (idr3, signer3)]:
        authnr.addIdr(idr, signer.verkey)

    def serz(req):
        return {k: v for k, v in req.as_dict.items()
                if k not in authnr.excluded_from_signing}

    op = randomOperation()
    request = Request(reqId=Request.gen_req_id(), operation=op,
                      protocolVersion=CURRENT_PROTOCOL_VERSION, identifier=idr1)
    req = wallet.sign_using_multi_sig(request=request, identifier=idr1)
    assert len(req.signatures) == 1
    req_data = serz(req)
    assert set(authnr.authenticate_multi(req_data, req.signatures, 1)) == {idr1, }
    with pytest.raises(InsufficientSignatures):
        authnr.authenticate_multi(req_data, req.signatures, 2)

    req = wallet.sign_using_multi_sig(request=req, identifier=idr2)
    assert len(req.signatures) == 2
    req_data = serz(req)
    assert set(authnr.authenticate_multi(req_data, req.signatures, 2)) == {idr1,
                                                                           idr2}
    with pytest.raises(InsufficientSignatures):
        authnr.authenticate_multi(req_data, req.signatures, 3)

    wallet.do_multi_sig_on_req(req, identifier=idr3)
    assert len(req.signatures) == 3
    req_data = serz(req)
    assert set(authnr.authenticate_multi(req_data, req.signatures, 3)) == {idr1,
                                                                           idr2,
                                                                           idr3}
