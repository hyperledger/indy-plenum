import pytest

from plenum.client.signer import SimpleSigner
from plenum.common.exceptions import InvalidSignature
from plenum.server.client_authn import SimpleAuthNr


@pytest.fixture(scope="module")
def cli():
    return SimpleSigner(424242)


@pytest.fixture(scope="module")
def sa(cli):
    sa = SimpleAuthNr()
    sa.addClient(cli.identifier, cli.verkey)
    return sa


@pytest.fixture(scope="module")
def msg():
    return dict(myMsg="42 (forty-two) is the natural number that succeeds 41 and precedes 43.")


@pytest.fixture(scope="module")
def sig(cli, msg):
    return cli.sign(msg)


def testClientAuthentication(sa, cli, msg, sig):
    sa.authenticate(msg, 424242, sig)


def testMessageModified(sa, cli, msg, sig):
    msg2 = msg.copy()

    # slight modification to the message
    msg2['myMsg'] = msg2['myMsg'][:-1] + '!'

    with pytest.raises(InvalidSignature):
        sa.authenticate(msg2, 424242, sig)


def testAnotherAuthenticatorCanAuthenticate(sa, cli, msg, sig):
    sa2 = SimpleAuthNr()
    sa2.addClient(cli.identifier, cli.verkey)
    sa.authenticate(msg, 424242, sig)


def testReconstitutedClientCreatesTheSameSig(cli, sig, msg):
    cli2 = SimpleSigner(424242, seed=cli.seed)
    sig2 = cli2.sign(msg)
    assert sig == sig2
