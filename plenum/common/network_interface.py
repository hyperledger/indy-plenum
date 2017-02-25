from abc import abstractmethod
from typing import Set, Tuple, Any, Union, Dict

from raet.nacling import Signer

from plenum.common.exceptions import RemoteNotFound
from plenum.common.log import getlogger
from plenum.common.request import Request
from plenum.common.types import TaggedTupleBase

logger = getlogger()


class NetworkInterface:
    localips = ['127.0.0.1', '0.0.0.0']

    # TODO: Does this serve the same purpose as `conns`, if yes then remove
    @property
    def connecteds(self) -> Set[str]:
        """
        Return the names of the nodes this node is connected to.
        """
        return {r.name for r in self.remotes.values()
                if self.isRemoteConnected(r)}

    @staticmethod
    @abstractmethod
    def isRemoteConnected(r) -> bool:
        """
        A node is considered to be connected if it is joined, allowed and alived.

        :param r: the remote to check
        """
        raise NotImplementedError

    def isConnectedTo(self, name: str = None, ha: Tuple = None):
        assert (name, ha).count(None) == 1, "One and only one of name or ha " \
                                            "should be passed. Passed " \
                                            "name: {}, ha: {}".format(name, ha)
        try:
            remote = self.getRemote(name, ha)
        except RemoteNotFound:
            return False
        return self.isRemoteConnected(remote)

    def getRemote(self, name: str = None, ha: Tuple = None):
        """
        Find the remote by name or ha.

        :param name: the name of the remote to find
        :param ha: host address pair the remote to find
        :raises: RemoteNotFound
        """
        assert (name, ha).count(None) == 1, "One and only one of name or ha " \
                                            "should be passed. Passed " \
                                            "name: {}, ha: {}".format(name, ha)
        remote = self.findInRemotesByName(name) if name else \
            self.findInRemotesByHA(ha)
        if not remote:
            raise RemoteNotFound(name or ha)
        return remote

    def findInRemotesByHA(self, remoteHa):
        remotes = [r for r in self.remotes.values()
                   if r.ha == remoteHa]
        assert len(remotes) <= 1, "Found remotes {}: {}". \
            format(len(remotes), [(r.name, r.ha) for r in remotes])
        if remotes:
            return remotes[0]
        return None

    def findInRemotesByName(self, name: str):
        """
        Find the remote by name.

        :param name: the name of the remote to find
        :raises: RemoteNotFound
        """
        try:
            return next(r for r in self.remotes.values()
                        if r.name == name)
        except StopIteration:
            return None

    def removeRemoteByName(self, name: str) -> int:
        """
        Remove the remote by name.

        :param name: the name of the remote to remove
        :raises: RemoteNotFound
        """
        remote = self.getRemote(name)
        rid = remote.uid
        self.removeRemote(remote)
        return rid

    # def send(self, msg: Any, remoteName: str):
    #     """
    #     Transmit the specified message to the remote specified by `remoteName`.
    #
    #     :param msg: a message
    #     :param remoteName: the name of the remote
    #     """
    #     rid = self.getRemote(remoteName).uid
    #     # Setting timeout to never expire
    #     self.transmit(msg, rid, timeout=self.messageTimeout)

    @property
    def conns(self) -> Set[str]:
        """
        Get the connections of this node.

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
            logger.info("{} disconnected from {}".format(self, o),
                        extra={"cli": "IMPORTANT"})
        for i in ins:
            logger.info("{} now connected to {}".format(self, i),
                        extra={"cli": "IMPORTANT"})

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

    def prepForSending(self, msg: Dict, signer: Signer = None) -> Dict:
        """
        Return a dictionary form of the message

        :param msg: the message to be sent
        :raises: ValueError if msg cannot be converted to an appropriate format
            for transmission
        """
        if isinstance(msg, TaggedTupleBase):
            tmsg = msg.melted()
        elif isinstance(msg, Request):
            tmsg = msg.__getstate__()
        elif hasattr(msg, "_asdict"):
            tmsg = dict(msg._asdict())
        elif hasattr(msg, "__dict__"):
            tmsg = dict(msg.__dict__)
        else:
            raise ValueError("Message cannot be converted to an appropriate "
                             "format for transmission")
        if signer:
            return self.sign(tmsg, signer)
        return tmsg

    def sameAddr(self, ha, ha2) -> bool:
        """
        Check whether the two arguments correspond to the same address
        """
        if ha == ha2:
            return True
        elif ha[1] != ha2[1]:
            return False
        else:
            return ha[0] in self.localips and ha2[0] in self.localips

    def sign(self, msg: Dict, signer: Signer) -> Dict:
        """
        No signing is implemented. Returns the msg as it is.
        An overriding class can define the signing implementation

        :param msg: the message to sign
        """
        return msg  # don't sign by default

    def remotesByConnected(self):
        """
        Partitions the remotes into connected and disconnected

        :return: tuple(connected remotes, disconnected remotes)
        """
        conns, disconns = [], []
        for r in self.remotes.values():
            array = conns if self.isRemoteConnected(r) else disconns
            array.append(r)
        return conns, disconns

