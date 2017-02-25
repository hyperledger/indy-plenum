import asyncio

import pytest
import zmq.asyncio

from plenum.common.looper import Looper
from plenum.common.port_dispenser import genHa
from plenum.common.temp_file_util import SafeTemporaryDirectory


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


@pytest.yield_fixture()
def tdirAndLooper(loop):
    asyncio.set_event_loop(loop)

    with SafeTemporaryDirectory() as td:
        with Looper(loop=loop, debug=True) as looper:
            yield td, looper


@pytest.fixture()
def tdir(tdirAndLooper):
    return tdirAndLooper[0]


@pytest.fixture()
def looper(tdirAndLooper):
    return tdirAndLooper[1]
