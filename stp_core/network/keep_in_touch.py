from abc import abstractmethod
from typing import Dict, Set

from stp_core.common.constants import CONNECTION_PREFIX
from stp_core.common.log import getlogger
from stp_core.ratchet import Ratchet
from stp_core.types import HA

logger = getlogger()


class KITNetworkInterface:
    # Keep In Touch Stack which maintains connections mentioned in
    # its registry
    def __init__(self, registry: Dict[str, HA]):
        self.registry = registry

        self.lastcheck = {}  # type: Dict[int, Tuple[int, float]]
        self.ratchet = Ratchet(a=8, b=0.198, c=-4, base=8, peak=3600)

        # holds the last time we checked remotes
        self.nextCheck = 0

    @abstractmethod
    def maintainConnections(self, force=False):
        """
        Ensure appropriate connections.

        """
        raise NotImplementedError

    @abstractmethod
    def reconcileNodeReg(self):
        raise NotImplementedError

    def serviceLifecycle(self) -> None:
        """
        Function that does the following activities if the node is going:
        (See `Status.going`)

        - check connections (See `checkConns`)
        - maintain connections (See `maintainConnections`)
        """
        self.checkConns()
        self.maintainConnections()

    @property
    def conns(self) -> Set[str]:
        """
        Get connections of this node which participate in the communication

        :return: set of names of the connected nodes
        """
        return self._conns

    @conns.setter
    def conns(self, value: Set[str]) -> None:
        """
        Updates the connection count of this node if not already done.
        """
        if not self._conns == value:
            old = self._conns
            self._conns = value
            ins = value - old
            outs = old - value
            logger.debug("{}'s connections changed from {} to {}".format(self,
                                                                         old,
                                                                         value))
            self._connsChanged(ins, outs)

    def checkConns(self):
        """
        Evaluate the connected nodes
        """
        self.conns = self.connecteds

    def _connsChanged(self, ins: Set[str], outs: Set[str]) -> None:
        """
        A series of operations to perform once a connection count has changed.

        - Set f to max number of failures this system can handle.
        - Set status to one of started, started_hungry or starting depending on
            the number of protocol instances.
        - Check protocol instances. See `checkProtocolInstaces()`

        :param ins: new nodes connected
        :param outs: nodes no longer connected
        """
        for o in outs:
            logger.info("{}{} disconnected from {}"
                        .format(CONNECTION_PREFIX, self, o),
                        extra={"cli": "IMPORTANT",
                               "tags": ["connected"]})
        for i in ins:
            logger.info("{}{} now connected to {}"
                        .format(CONNECTION_PREFIX, self, i),
                        extra={"cli": "IMPORTANT",
                               "tags": ["connected"]})

            # remove remotes for same ha when a connection is made
            remote = self.getRemote(i)
            others = [r for r in self.remotes.values()
                      if r.ha == remote.ha and r.name != i]
            for o in others:
                logger.debug("{} removing other remote".format(self))
                self.removeRemote(o)

        self.onConnsChanged(ins, outs)

    def onConnsChanged(self, ins: Set[str], outs: Set[str]):
        """
        Subclasses can override
        """
        pass

    def findInNodeRegByHA(self, remoteHa):
        """
        Returns the name of the remote by HA if found in the node registry, else
        returns None
        """
        regName = [nm for nm, ha in self.registry.items()
                   if self.sameAddr(ha, remoteHa)]
        if len(regName) > 1:
            raise RuntimeError("more than one node registry entry with the "
                               "same ha {}: {}".format(remoteHa, regName))
        if regName:
            return regName[0]
        return None

    def getRemoteName(self, remote):
        """
        Returns the name of the remote object if found in node registry.

        :param remote: the remote object
        """
        if remote.name not in self.registry:
            find = [name for name, ha in self.registry.items()
                    if ha == remote.ha]
            assert len(find) == 1
            return find[0]
        return remote.name

    @property
    def notConnectedNodes(self) -> Set[str]:
        """
        Returns the names of nodes in the registry this node is NOT connected
        to.
        """
        return set(self.registry.keys()) - self.conns
