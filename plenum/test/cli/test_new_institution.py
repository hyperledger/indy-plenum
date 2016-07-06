import pytest

from plenum.test.cli.helper import newCli


@pytest.fixture()
def BYUCli(nodeRegsForCLI, looper, tdir):
    return newCli(nodeRegsForCLI, looper, tdir)


@pytest.fixture()
def philCli(nodeRegsForCLI, looper, tdir):
    return newCli(nodeRegsForCLI, looper, tdir)


@pytest.fixture()
def tylerCli(nodeRegsForCLI, looper, tdir):
    return newCli(nodeRegsForCLI, looper, tdir)


@pytest.fixture()
def new_keypair(BYUCli):
    # TODO create a fixture: Phil is already a sponsor
    BYUCli.enterCmd("new_keypair")
    assert len(BYUCli.defaultClient.signers) == 2
    BYUPubKeyMsg = BYUCli.lastPrintArgs['msg']
    assert BYUPubKeyMsg.startswith('Public key')
    BYUPubKey = BYUPubKeyMsg.split(" ")[-1]
    BYUCli.enterCmd("list ids")
    # TODO check the results of list ids
    return BYUPubKey


def testNewKeypair(new_keypair):
    pass


@pytest.fixture()
def newInstitution(new_keypair, philCli):
    BYUPubKey = new_keypair
    """
    Test to demonstrate anonymous credentials through Sovrin CLI.
    """
    philCli.enterCmd("send NYM dest={}".format(BYUPubKey))  # TODO incomplete
    philCli.enterCmd("send GET_NYM dest={}".format(BYUPubKey))  # TODO incomplete
    philCli.enterCmd("send ATTRIB dest={key} "
                     "raw={{email: mail@byu.edu}}".format(key=BYUPubKey))
    # TODO verify valid responses for above commands


def testNewInstitution(newInstitution):
    pass


@pytest.fixture()
def createCredDef(BYUCli, newInstitution):
    BYUCli.enterCmd(
        'send CRED_DEF name="Qualifications" version="1.0" type=JC1 '
        'ip=10.10.10.10 port=7897 keys={master_secret:<large number>, '
        'n:<large number>, S:<large number>, Z:<large number>, '
        'attributes: {'
        '"first_name":R1, "last_name":R2, "birth_date":R3, "expire_date":R4, '
        '"undergrad":R5, "postgrad":R6}}')
    # TODO check we get a valid response


@pytest.fixture()
def tylerKeypairForBYU(tylerCli):
    tylerCli.enterCmd('new keypair BYU')
    TylerPubKeyMsg = tylerCli.lastPrintArgs['msg']
    assert TylerPubKeyMsg.startswith('Public key')
    TylerPubKey = TylerPubKeyMsg.split(" ")[-1]
    # TODO more check that it's a valid key
    return TylerPubKey


def testAnonCredsCLI(createAllNodes, cli, new_keypair, new_steward,
                     createCredDef,
                     tylerKeypairForBYU):
    BYUPubKey = new_keypair
    TylerPubKey = tylerKeypairForBYU
    cli.enterCmd('use keypair {}'.format(BYUPubKey))
    assert cli.activeSigner.verstr == BYUPubKey
    cli.enterCmd("send NYM dest={}".format(TylerPubKey))  # TODO incomplete
    cli.enterCmd("send GET_NYM dest={}".format(TylerPubKey))  # TODO incomplete
    cli.enterCmd("become {}".format(TylerPubKey))
    assert cli.activeSigner.verstr == TylerPubKey
    cli.enterCmd("send to {} saveas BYU-QUAL REQ_CRED name=Qualifications"
                 " version=1.0 attrs=undergrad,postgrad".format(BYUPubKey))
    cli.enterCmd("list CRED")
    cli.enterCmd("become {}".format(TylerPubKey))
    # TODO Verifier: BookStore must already exist on Sovrin
    bookStorePubKey = None
    cli.enterCmd("send proof of undergrad from CRED-BYU-QUAL to"
                 " {}".format(bookStorePubKey))

