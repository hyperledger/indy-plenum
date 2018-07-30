import pytest

from stp_core.loop.eventually import eventually
from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import Printer, prepStacks, CounterMsgsHandler, connectStack
from stp_zmq.test.helper import genKeys, check_pong_received
from stp_zmq.zstack import ZStack


@pytest.fixture()
def create_stacks(tdir, looper):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)

    alphaP = Printer(names[0])
    alpha = ZStack(names[0],
                   ha=genHa(),
                   basedirpath=tdir,
                   msgHandler=alphaP.print,
                   restricted=True)
    prepStacks(looper, alpha, connect=False)

    beta_msg_handler = CounterMsgsHandler()
    beta = ZStack(names[1],
                  ha=genHa(),
                  basedirpath=tdir,
                  msgHandler=beta_msg_handler.handler,
                  restricted=True)
    prepStacks(looper, beta, connect=False)

    return alpha, beta


def check_stashed_pongs(looper, stack, to):
    def do_check_stashed_pongs():
        assert to in stack._stashed_pongs

    looper.run(eventually(do_check_stashed_pongs))


def check_no_stashed_pongs(looper, stack, to):
    def do_check_stashed_pongs():
        assert to not in stack._stashed_pongs

    looper.run(eventually(do_check_stashed_pongs))


def test_stash_pongs_from_unknown(looper, create_stacks):
    '''
    Beta should stash the pong for Alpha on Alpha's ping received when
    Alpha is not connected yet.
    '''
    alpha, beta = create_stacks
    connectStack(alpha, beta)
    alpha.sendPingPong(beta.name, is_ping=True)

    check_stashed_pongs(looper, stack=beta, to=alpha.publicKey)

    connectStack(beta, alpha)
    check_no_stashed_pongs(looper, stack=beta, to=alpha.publicKey)

    check_pong_received(looper, alpha, beta.name)
