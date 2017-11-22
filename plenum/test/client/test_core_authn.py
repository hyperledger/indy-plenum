import pytest

from plenum.common.constants import GET_TXN, NODE, NYM
from plenum.common.exceptions import CouldNotAuthenticate, \
    InsufficientSignatures, InsufficientCorrectSignatures, MissingSignature
from plenum.common.signer_simple import SimpleSigner
from plenum.common.types import f
from plenum.server.client_authn import CoreAuthNr


idr = '5G72199XZB7wREviUbQma7'
msg_str = "42 (forty-two) is the natural number that succeeds 41 and precedes 43."


class DummyAuthenticator(CoreAuthNr):
    def getVerkey(self, _):
        return None


@pytest.fixture(scope="module")
def signer():
    return SimpleSigner(idr)


@pytest.fixture(scope="module")
def sa(signer):
    sa = CoreAuthNr()
    sa.addIdr(signer.identifier, signer.verkey)
    return sa


@pytest.fixture(scope="module")
def msg():
    return {'myMsg': msg_str, f.IDENTIFIER.nm: idr}


@pytest.fixture(scope="module")
def sig(signer, msg):
    return signer.sign(msg)


def test_authenticate_raises_correct_exception():
    msg = dict(myMsg=msg_str)
    simple_signer = SimpleSigner()
    identifier = simple_signer.identifier
    verkey = simple_signer.verkey
    dummyAr = DummyAuthenticator()
    dummyAr.addIdr(identifier, verkey)
    pytest.raises(MissingSignature, dummyAr.authenticate,
                  msg, identifier)


def testClientAuthentication(sa, signer, msg, sig):
    assert sa.authenticate(msg, idr, sig)


def testMessageModified(sa, signer, msg, sig):
    msg2 = msg.copy()

    # slight modification to the message
    msg2['myMsg'] = msg2['myMsg'][:-1] + '!'

    with pytest.raises(InsufficientCorrectSignatures):
        sa.authenticate(msg2, idr, sig)


def testAnotherAuthenticatorCanAuthenticate(sa, signer, msg, sig):
    sa2 = CoreAuthNr()
    sa2.addIdr(signer.identifier, signer.verkey)
    assert sa.authenticate(msg, idr, sig)


def testReconstitutedClientCreatesTheSameSig(signer, sig, msg):
    signer2 = SimpleSigner(idr, seed=signer.seed)
    sig2 = signer2.sign(msg)
    assert sig == sig2


@pytest.fixture(scope="module")
def signer2():
    return SimpleSigner()


@pytest.fixture(scope="module")
def signer3():
    return SimpleSigner()


@pytest.fixture(scope="module")
def signer4():
    return SimpleSigner()


@pytest.fixture(scope="module")
def multi_sa(sa, signer2, signer3, signer4):
    for c in (signer2, signer3, signer4):
        sa.addIdr(c.identifier, c.verkey)
    return sa


@pytest.fixture(scope="module")
def correct_sigs(msg, signer, signer2, signer3, signer4):
    return {c.identifier: c.sign(msg) for c in (signer, signer2, signer3, signer4)}


def test_verify_multi_sig_correct(multi_sa, msg, signer, signer2, signer3, signer4,
                                  correct_sigs):
    idrs = correct_sigs.keys()
    assert list(idrs) == multi_sa.authenticate_multi(msg, correct_sigs)
    for i in range(1, 5):
        # `authenticate_multi` returns threshold number of identifiers on success
        assert len(set(
            multi_sa.authenticate_multi(msg, correct_sigs, i)
        ).intersection(set(idrs))) == i


@pytest.fixture(scope="module")
def two_correct_sigs(msg, signer, signer2, signer3, signer4):
    correct = {c.identifier: c.sign(msg) for c in (signer, signer2)}
    incorrect = {c.identifier: c.sign({**msg, 'random_key': 11}) for c in (signer3, signer4)}
    return {**correct, **incorrect}


def test_verify_multi_sig_threshold(multi_sa, msg, signer, signer2, signer3,
                                    signer4, two_correct_sigs):
    idrs = {signer.identifier, signer2.identifier}
    for i in range(1, 3):
        # `authenticate_multi` returns threshold number of identifiers on success
        assert len(set(
            multi_sa.authenticate_multi(msg, two_correct_sigs, i)
        ).intersection(idrs)) == i

    with pytest.raises(InsufficientCorrectSignatures):
        assert multi_sa.authenticate_multi(msg, two_correct_sigs, 3)

    with pytest.raises(InsufficientCorrectSignatures):
        assert multi_sa.authenticate_multi(msg, two_correct_sigs, 4)

    with pytest.raises(InsufficientCorrectSignatures):
        multi_sa.authenticate_multi(msg, two_correct_sigs)

    with pytest.raises(InsufficientSignatures):
        sigs = {c.identifier: c.sign(msg) for c in (signer, signer2)}
        multi_sa.authenticate_multi(msg, sigs, 3)


def test_txn_types(sa):
    assert sa.is_query(GET_TXN)
    assert not sa.is_write(GET_TXN)
    assert sa.is_write(NODE)
    assert not sa.is_query(NODE)
    assert sa.is_write(NYM)
    assert not sa.is_query(NYM)
