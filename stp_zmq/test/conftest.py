import asyncio
import logging
import pytest
import zmq.asyncio

from stp_core.common.log import getlogger
from stp_core.common.config.util import getConfig
from stp_core.common.temp_file_util import SafeTemporaryDirectory
from stp_core.loop.looper import Looper

from stp_core.network.port_dispenser import genHa


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
