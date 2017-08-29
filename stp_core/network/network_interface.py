import time
from abc import abstractmethod, ABCMeta
from typing import Set

from stp_core.common.log import getlogger
from stp_core.network.exceptions import RemoteNotFound, DuplicateRemotes
from stp_core.types import HA

logger = getlogger()


# TODO: There a number of methods related to keys management, they can be moved to some class like KeysManager
class NetworkInterface(metaclass=ABCMeta):
    localips = ['127.0.0.1', '0.0.0.0']

    @property
    @abstractmethod
    def remotes(self):
        """
        Return all remote nodes (both connected and not)
        """
        pass

    @property
    @abstractmethod
    def created(self):
        pass

    @property
    @abstractmethod
    def name(self):
        pass

    @staticmethod
    @abstractmethod
    def isRemoteConnected(r) -> bool:
        """
        A node is considered to be connected if it is joined, allowed and alive

        :param r: the remote to check
        """
        pass

    @staticmethod
    @abstractmethod
    def initLocalKeys(name, baseDir, sigseed, override=False):
        pass

    @staticmethod
    @abstractmethod
    def initRemoteKeys(name, remoteName, baseDir, verkey, override=False):
        pass

    @abstractmethod
    def onHostAddressChanged(self):
        pass

    @staticmethod
    @abstractmethod
    def areKeysSetup(name, baseDir):
        pass

    @staticmethod
    @abstractmethod
    def learnKeysFromOthers(baseDir, name, others):
        pass

    @abstractmethod
    def tellKeysToOthers(self, others):
        pass

    @staticmethod
    @abstractmethod
    def getHaFromLocal(name, basedirpath):
        pass

    @abstractmethod
    def removeRemote(self, r):
        pass

    @abstractmethod
    def transmit(self, msg, uid, timeout=None):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def connect(self, name=None, remoteId=None, ha=None, verKeyRaw=None,
                publicKeyRaw=None):
        pass

    @abstractmethod
    def send(self, msg, remote: str = None, ha=None):
        pass

    def connectIfNotConnected(self, name=None, remoteId=None, ha=None,
                              verKeyRaw=None, publicKeyRaw=None):
        if not self.isConnectedTo(name=name, ha=ha):
            self.connect(name=name, remoteId=remoteId, ha=ha,
                         verKeyRaw=verKeyRaw, publicKeyRaw=publicKeyRaw)
        else:
            logger.debug('{} already connected {}'.format(self.name, ha))

    # TODO: Does this serve the same purpose as `conns`, if yes then remove
    @property
    def connecteds(self) -> Set[str]:
        """
        Return the names of the remote nodes this node is connected to.
        Not all of these nodes may be used for communication
         (as opposed to conns property)
        """
        return {r.name for r in self.remotes.values()
                if self.isRemoteConnected(r)}

    @property
    def age(self):
        """
        Returns the time elapsed since this stack was created
        """
        return time.perf_counter() - self.created

    def isConnectedTo(self, name: str = None, ha: HA = None):
        try:
            remote = self.getRemote(name, ha)
        except RemoteNotFound:
            return False
        return self.isRemoteConnected(remote)

    def getRemote(self, name: str = None, ha: HA = None):
        """
        Find the remote by name or ha.

        :param name: the name of the remote to find
        :param ha: host address pair the remote to find
        :raises: RemoteNotFound
        """
        return self.findInRemotesByName(name) if name else \
            self.findInRemotesByHA(ha)

    def findInRemotesByHA(self, remoteHa: HA):
        remotes = [r for r in self.remotes.values()
                   if r.ha == remoteHa]
        if len(remotes) > 1:
            raise DuplicateRemotes(remotes)
        if not remotes:
            raise RemoteNotFound(remoteHa)
        return remotes[0]

    def findInRemotesByName(self, name: str):
        """
        Find the remote by name.

        :param name: the name of the remote to find
        :raises: RemoteNotFound
        """
        remotes = [r for r in self.remotes.values()
                   if r.name == name]
        if len(remotes) > 1:
            raise DuplicateRemotes(remotes)
        if not remotes:
            raise RemoteNotFound(name)
        return remotes[0]

    def hasRemote(self, name):
        try:
            self.getRemote(name=name)
            return True
        except RemoteNotFound:
            return False

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

    def getHa(self, name):
        try:
            remote = self.getRemote(name)
        except RemoteNotFound:
            return None
        return remote.ha

    def sameAddr(self, ha, ha2) -> bool:
        """
        Check whether the two arguments correspond to the same address
        """
        if ha == ha2:
            return True
        if ha[1] != ha2[1]:
            return False
        return ha[0] in self.localips and ha2[0] in self.localips

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
