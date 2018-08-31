from functools import partial
from typing import Any, Optional, NamedTuple

from stp_core.network.network_interface import NetworkInterface
from stp_zmq.zstack import ZStack, Quota
from stp_core.types import HA

from stp_core.loop.eventually import eventuallyAll, eventually
from plenum.common.exceptions import NotConnectedToAny
from stp_core.common.log import getlogger
from plenum.test.exceptions import NotFullyConnected
from plenum.test.stasher import Stasher
from plenum.test import waits

logger = getlogger()

BaseStackClass = ZStack


class TestStack(BaseStackClass):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stasher = Stasher(self.rxMsgs,
                               "TestStack~" + self.name)

        self.delay = self.stasher.delay

    # def _serviceStack(self, age):
    #     super()._serviceStack(age)
    #     self.stasher.process(age)

    async def _serviceStack(self, age, quota: Quota):
        await super()._serviceStack(age, quota)
        self.stasher.process(age)

    def resetDelays(self):
        self.stasher.resetDelays()

    def force_process_delayeds(self, *names):
        return self.stasher.force_unstash(*names)


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
RemoteState = NamedTuple("RemoteState", [
    ('isConnected', Optional[bool])
])

CONNECTED = RemoteState(isConnected=True)
NOT_CONNECTED = RemoteState(isConnected=False)
# TODO this is to allow imports to pass until we create abstractions for
# ZMQ
JOINED_NOT_ALLOWED = RemoteState(isConnected=False)
JOINED = RemoteState(isConnected=False)


def checkState(state: RemoteState, obj: Any, details: str = None):
    if state is not None:
        checkedItems = {}
        for key, s in state._asdict().items():
            checkedItems[key] = 'N/A' if s == 'N/A' else getattr(obj, key)
        actualState = RemoteState(**checkedItems)
        assert actualState == state, '{} has unexpected state'.format(details)


def checkRemoteExists(frm: ZStack,
                      to: str,  # remoteName
                      state: Optional[RemoteState] = None):
    remote = frm.getRemote(to)
    checkState(state, remote, "{}'s remote {}".format(frm.name, to))
