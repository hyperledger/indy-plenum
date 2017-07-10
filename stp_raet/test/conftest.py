import asyncio

from stp_core.common.temp_file_util import SafeTemporaryDirectory
from stp_core.loop.looper import Looper
from stp_core.network.port_dispenser import genHa
from stp_core.test.conftest import *


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
    loop = asyncio.get_event_loop()
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