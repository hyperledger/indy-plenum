import asyncio
import logging
import pytest
import zmq.asyncio

from plenum.common.stacks import ClientZStack
from plenum.test.helper import MockTimer
from stp_core.common.log import getlogger
from stp_core.common.config.util import getConfig
from stp_core.common.temp_file_util import SafeTemporaryDirectory
from stp_core.crypto.util import randomSeed
from stp_core.loop.looper import Looper

from stp_core.network.port_dispenser import genHa
from stp_core.test.helper import SMotor
from stp_zmq.simple_zstack import SimpleZStack
from stp_zmq.test.helper import genKeys


@pytest.fixture()
def registry():
    return {
        'Alpha': genHa(),
        'Beta': genHa(),
        'Gamma': genHa(),
        'Delta': genHa()
    }


@pytest.fixture()
def loop():
    loop = zmq.asyncio.ZMQEventLoop()
    loop.set_debug(True)
    return loop


@pytest.yield_fixture()
def tdirAndLooper(loop):
    asyncio.set_event_loop(loop)

    with SafeTemporaryDirectory() as td:
        with Looper(loop=loop) as looper:
            yield td, looper


@pytest.fixture()
def tdir(tdirAndLooper):
    return tdirAndLooper[0]


@pytest.fixture()
def looper(tdirAndLooper):
    return tdirAndLooper[1]


BIG_NUM_OF_MSGS = 100000


@pytest.fixture()
def tconf():
    tmp = getConfig()
    old_node_num = tmp.ZMQ_NODE_QUEUE_SIZE
    old_client_num = tmp.ZMQ_CLIENT_QUEUE_SIZE
    tmp.ZMQ_NODE_QUEUE_SIZE = BIG_NUM_OF_MSGS
    tmp.ZMQ_CLIENT_QUEUE_SIZE = BIG_NUM_OF_MSGS
    yield tmp
    tmp.ZMQ_NODE_QUEUE_SIZE = old_node_num
    tmp.ZMQ_CLIENT_QUEUE_SIZE = old_client_num


@pytest.fixture(scope="module")
def set_info_log_level():
    logger = getlogger()
    lvl = logger.level
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(lvl)


@pytest.fixture()
def alpha_handler(tdir, looper):
    return Handler()


@pytest.fixture()
def stacks(tdir, looper, alpha_handler):
    names = ['Alpha', 'Beta']
    genKeys(tdir, names)
    aseed = randomSeed()
    bseed = randomSeed()

    def bHandler(m):
        msg, a = m
        beta.send(msg, a)

    stackParams = {
        "name": names[0],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    timer = MockTimer(0)
    alpha = SimpleZStack(stackParams, alpha_handler.handle, aseed, False,
                         timer=timer)

    stackParams = {
        "name": names[1],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    timer = MockTimer(0)
    beta = SimpleZStack(stackParams, bHandler, bseed, True,
                        timer=timer)

    amotor = SMotor(alpha)
    looper.add(amotor)

    bmotor = SMotor(beta)
    looper.add(bmotor)
    return alpha, beta


@pytest.fixture()
def clientstack(tdir, looper, alpha_handler):
    names = ['ClientA', 'Alpha']
    genKeys(tdir, names)
    aseed = randomSeed()
    cseed = randomSeed()

    stackParams = {
        "name": names[0],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    timer = MockTimer(0)
    client = ClientZStack(stackParams, alpha_handler.handle, cseed, False,
                          timer=timer)

    stackParams = {
        "name": names[1],
        "ha": genHa(),
        "auto": 2,
        "basedirpath": tdir
    }
    timer = MockTimer(0)
    alpha = SimpleZStack(stackParams, alpha_handler, aseed, True,
                         timer=timer)

    amotor = SMotor(alpha)
    looper.add(amotor)

    cmotor = SMotor(client)
    looper.add(cmotor)
    return alpha, client


class Handler:
    def __init__(self) -> None:
        self.received_messages = []

    def handle(self, m):
        d, msg = m
        self.received_messages.append(d)
