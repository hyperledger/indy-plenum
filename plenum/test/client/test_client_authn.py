import pytest

from plenum.common.exceptions import InvalidSignature, CouldNotAuthenticate, \
    InsufficientCorrectSignatures
from plenum.common.signer_simple import SimpleSigner
from plenum.server.client_authn import SimpleAuthNr

idr = '5G72199XZB7wREviUbQma7'
msg_str = "42 (forty-two) is the natural number that succeeds 41 and precedes 43."


class DummyAuthenticator(SimpleAuthNr):
    def getVerkey(self, _):
        return None


@pytest.fixture(scope="module")
def cli():
    return SimpleSigner(idr)


@pytest.fixture(scope="module")
def sa(cli):
    sa = SimpleAuthNr()
    sa.addIdr(cli.identifier, cli.verkey)
    return sa


@pytest.fixture(scope="module")
def msg():
    return dict(myMsg=msg_str)


@pytest.fixture(scope="module")
def sig(cli, msg):
    return cli.sign(msg)


def test_authenticate_raises_correct_exception():
    msg = dict(myMsg=msg_str)
    simple_signer = SimpleSigner()
    identifier = simple_signer.identifier
    signature = simple_signer.sign(msg)
    verkey = simple_signer.verkey
    dummyAr = DummyAuthenticator()
    dummyAr.addIdr(identifier, verkey)
    pytest.raises(CouldNotAuthenticate, dummyAr.authenticate,
                  msg, identifier, signature)


def testClientAuthentication(sa, cli, msg, sig):
    sa.authenticate(msg, idr, sig)


def testMessageModified(sa, cli, msg, sig):
    msg2 = msg.copy()

    # slight modification to the message
    msg2['myMsg'] = msg2['myMsg'][:-1] + '!'

    with pytest.raises(InsufficientCorrectSignatures):
        sa.authenticate(msg2, idr, sig)


def testAnotherAuthenticatorCanAuthenticate(sa, cli, msg, sig):
    sa2 = SimpleAuthNr()
    sa2.addIdr(cli.identifier, cli.verkey)
    sa.authenticate(msg, idr, sig)


def testReconstitutedClientCreatesTheSameSig(cli, sig, msg):
    cli2 = SimpleSigner(idr, seed=cli.seed)
    sig2 = cli2.sign(msg)
    assert sig == sig2
