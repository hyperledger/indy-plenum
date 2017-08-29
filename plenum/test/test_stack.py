from functools import partial
from typing import Any, Optional, NamedTuple

from stp_core.network.network_interface import NetworkInterface
from stp_raet.rstack import RStack
from stp_zmq.zstack import ZStack
from stp_core.types import HA

from plenum.common.config_util import getConfig
from stp_core.loop.eventually import eventuallyAll, eventually
from plenum.common.exceptions import NotConnectedToAny
from stp_core.common.log import getlogger
from plenum.test.exceptions import NotFullyConnected
from plenum.test.stasher import Stasher
from plenum.test import waits
from plenum.common import util

logger = getlogger()
config = getConfig()


if config.UseZStack:
    BaseStackClass = ZStack
else:
    BaseStackClass = RStack


class TestStack(BaseStackClass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stasher = Stasher(self.rxMsgs,
                               "TestStack~" + self.name)

        self.delay = self.stasher.delay

    # def _serviceStack(self, age):
    #     super()._serviceStack(age)
    #     self.stasher.process(age)

    async def _serviceStack(self, age):
        await super()._serviceStack(age)
        self.stasher.process(age)

    def resetDelays(self):
        self.stasher.resetDelays()

    def force_process_delayeds(self):
        return self.stasher.force_unstash()


class StackedTester:
    def checkIfConnectedTo(self, count=None):
        connected = set()
        # TODO refactor to not use values
        for address in self.nodeReg.values():
            for remote in self.nodestack.remotes.values():
                if HA(*remote.ha) == address:
                    if BaseStackClass.isRemoteConnected(remote):
                        connected.add(remote.name)
                        break
        allRemotes = set(self.nodeReg)
        totalNodes = len(self.nodeReg) if count is None else count
        if count is None and len(connected) == 0:
            raise NotConnectedToAny(allRemotes)
        elif len(connected) < totalNodes:
            raise NotFullyConnected(allRemotes - connected)
        else:
            assert len(connected) == totalNodes

    async def ensureConnectedToNodes(self, customTimeout=None, count=None):
        timeout = customTimeout or \
            waits.expectedClientToPoolConnectionTimeout(len(self.nodeReg))

        logger.debug(
            "waiting for {} seconds to check client connections to "
            "nodes...".format(timeout))
        chk_connected = partial(self.checkIfConnectedTo, count)
        await eventuallyAll(chk_connected,
                            retryWait=.5,
                            totalTimeout=timeout)

    async def ensureDisconnectedToNodes(self, timeout):
        # TODO is this used? If so - add timeout for it to plenum.test.waits
        await eventually(self.checkIfConnectedTo, 0,
                         retryWait=.5,
                         timeout=timeout)


def getTestableStack(stack: NetworkInterface):
    """
    Dynamically modify a class that extends from `RStack` and introduce
    `TestStack` in the class hierarchy
    :param stack:
    :return:
    """
    # TODO: Can it be achieved without this mro manipulation?
    mro = stack.__mro__
    newMro = []
    for c in mro[1:]:
        if c == BaseStackClass:
            newMro.append(TestStack)
        newMro.append(c)
    return type(stack.__name__, tuple(newMro), dict(stack.__dict__))


# TODO: move to stp
if config.UseZStack:
    RemoteState = NamedTuple("RemoteState", [
        ('isConnected', Optional[bool])
    ])

    CONNECTED = RemoteState(isConnected=True)
    NOT_CONNECTED = RemoteState(isConnected=False)
    # TODO this is to allow imports to pass until we create abstractions for
    # RAET and ZMQ
    JOINED_NOT_ALLOWED = RemoteState(isConnected=False)
    JOINED = RemoteState(isConnected=False)
else:
    RemoteState = NamedTuple("RemoteState", [
        ('joined', Optional[bool]),
        ('allowed', Optional[bool]),
        ('alived', Optional[bool])])

    CONNECTED = RemoteState(joined=True, allowed=True, alived=True)
    NOT_CONNECTED = RemoteState(joined=None, allowed=None, alived=None)
    JOINED_NOT_ALLOWED = RemoteState(joined=True, allowed=None, alived=None)
    JOINED = RemoteState(joined=True, allowed='N/A', alived='N/A')


def checkState(state: RemoteState, obj: Any, details: str=None):
    if state is not None:
        checkedItems = {}
        for key, s in state._asdict().items():
            checkedItems[key] = 'N/A' if s == 'N/A' else getattr(obj, key)
        actualState = RemoteState(**checkedItems)
        assert actualState == state, set(actualState._asdict().items()) - \
            set(state._asdict().items())


def checkRemoteExists(frm: RStack,
                      to: str,  # remoteName
                      state: Optional[RemoteState] = None):
    remote = frm.getRemote(to)
    checkState(state, remote, "{}'s remote {}".format(frm.name, to))
