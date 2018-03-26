import os
import tempfile

import logging
import portalocker

from stp_core.types import HA
from stp_core.network.util import checkPortAvailable


class PortDispenser:
    """
    This class provides a system-wide mechanism to provide a available socket
    ports for testing. Tests should call getNext to get the next available port.
    There is no guarantee of sequential port numbers, as other tests running
    concurrently might grab a port before one process is done getting all the
    ports it needs. This should pose no problem, as tests shouldn't depend on
    port numbers. It leverages the filesystem lock mechanism to ensure there
    are no overlaps.
    """

    maxportretries = 3
    logger = logging.getLogger()

    def __init__(self, ip: str, filename: str=None, minPort=6000, maxPort=9999):
        self.ip = ip
        self.FILE = filename or os.path.join(tempfile.gettempdir(),
                                             'stp-portmutex.{}.txt'.format(ip))
        self.minPort = minPort
        self.maxPort = maxPort
        self.initFile()

    def initFile(self):
        if not os.path.exists(self.FILE):
            with open(self.FILE, "w") as file:
                file.write(str(self.minPort))

    def get(self, count: int=1, readOnly: bool=False, recurlvl=0):
        with open(self.FILE, "r+") as file:
            portalocker.lock(file, portalocker.LOCK_EX)
            ports = []
            while len(ports) < count:
                file.seek(0)
                port = int(file.readline())
                if readOnly:
                    return port
                port += 1
                if port > self.maxPort:
                    port = self.minPort
                file.seek(0)
                file.write(str(port))
                try:
                    checkPortAvailable(("", port))
                    ports.append(port)
                    self.logger.debug("new port dispensed: {}".format(port))
                except Exception:
                    if recurlvl < self.maxportretries:
                        self.logger.debug("port {} unavailable, trying again...".
                                          format(port))
                        recurlvl += 1
                    else:
                        self.logger.debug("port {} unavailable, max retries {} "
                                          "reached".
                                          format(port, self.maxportretries))
                        raise
            return ports

    def getNext(self, count: int=1, ip=None):
        ip = ip or self.ip
        has = [HA(ip, port) for port in self.get(count)]
        if len(has) == 1:
            return has[0]
        else:
            return has


genHa = PortDispenser("127.0.0.1").getNext
