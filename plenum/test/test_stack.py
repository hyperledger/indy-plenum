from typing import Any, Optional, NamedTuple

from plenum.common.eventually import eventuallyAll, eventually
from plenum.common.log import getlogger
from plenum.common.stacked import Stack, KITStack
from plenum.common.types import HA
from plenum.test.exceptions import NotFullyConnected
from plenum.common.exceptions import NotConnectedToAny
from plenum.test.stasher import Stasher
from plenum.test.waits import expectedWait
from raet.raeting import TrnsKind


logger = getlogger()


class TestStack(Stack):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stasher = Stasher(self.rxMsgs,
                               "TestStack~" + self.name)

        self.delay = self.stasher.delay
        self.declinedJoins = 0

    def _serviceStack(self, age):
        super()._serviceStack(age)
        self.stasher.process(age)

    def resetDelays(self):
        self.stasher.resetDelays()


class TestKITStack(KITStack):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.declinedJoins = 0

    def processRx(self, packet):
        # Override to add check that in case of join new remote is in registry. This is done to avoid creation
        # of unnecessary JSON files for remotes
        tk = packet.data['tk']

        if tk in [TrnsKind.join]:  # join transaction
            sha = (packet.data['sh'], packet.data['sp'])
            if not super().findInNodeRegByHA(sha):
                logger.debug('Remote with HA {} not found in registry'.format(sha))
                self.declinedJoins += 1
                return

        return super().processRx(packet)


class StackedTester:
    def checkIfConnectedTo(self, count=None):
        connected = 0
        # TODO refactor to not use values
        for address in self.nodeReg.values():
            for remote in self.nodestack.remotes.values():
                if HA(*remote.ha) == address:
                    if Stack.isRemoteConnected(remote):
                        connected += 1
                        break
        totalNodes = len(self.nodeReg) if count is None else count
        if count is None and connected == 0:
            raise NotConnectedToAny()
        elif connected < totalNodes:
            raise NotFullyConnected()
        else:
            assert connected == totalNodes

    async def ensureConnectedToNodes(self, timeout=None):
        wait = timeout or expectedWait(len(self.nodeReg))
        logger.debug(
                "waiting for {} seconds to check client connections to "
                "nodes...".format(wait))
        await eventuallyAll(self.checkIfConnectedTo, retryWait=.5,
                            totalTimeout=wait)

    async def ensureDisconnectedToNodes(self, timeout):
        await eventually(self.checkIfConnectedTo, 0, retryWait=.5,
                         timeout=timeout)


def getTestableStack(stack: Stack):
    """
    Dynamically modify a class that extends from `Stack` and introduce
    `TestStack` in the class hierarchy
    :param stack:
    :return:
    """
    mro = stack.__mro__
    newMro = []
    for c in mro[1:]:
        if c == Stack:
            newMro.append(TestStack)
        if c == KITStack:
            newMro.append(TestKITStack)
        newMro.append(c)
    return type(stack.__name__, tuple(newMro), dict(stack.__dict__))


RemoteState = NamedTuple("RemoteState", [
    ('joined', Optional[bool]),
    ('allowed', Optional[bool]),
    ('alived', Optional[bool])])


def checkState(state: RemoteState, obj: Any, details: str=None):
    if state is not None:
        checkedItems = {}
        for key, s in state._asdict().items():
            checkedItems[key] = 'N/A' if s == 'N/A' else getattr(obj, key)
        actualState = RemoteState(**checkedItems)
        assert actualState == state, set(actualState._asdict().items()) - \
                                     set(state._asdict().items())


def checkRemoteExists(frm: Stack,
                      to: str,  # remoteName
                      state: Optional[RemoteState] = None):
    remote = frm.getRemote(to)
    checkState(state, remote, "{}'s remote {}".format(frm.name, to))


CONNECTED = RemoteState(joined=True, allowed=True, alived=True)
NOT_CONNECTED = RemoteState(joined=None, allowed=None, alived=None)
JOINED_NOT_ALLOWED = RemoteState(joined=True, allowed=None, alived=None)
JOINED = RemoteState(joined=True, allowed='N/A', alived='N/A')