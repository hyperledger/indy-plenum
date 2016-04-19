import logging
import os
import sys
import tempfile

import portalocker
from ioflo.base.consoling import getConsole

from plenum.common.types import HA
from plenum.common.util import error, addTraceToLogging, TRACE_LOG_LEVEL, \
    checkPortAvailable, getlogger

logger = getlogger()


def checkDblImp():
    """
    Added this because I spent the better part of an evening troubleshooting an
    issue cause by double import. We were importing test.helper in one
    place, and test_helper in another, and python sees them as two different
    modules, and imported the same file twice. This caused genHa to be loaded
    twice, which caused overlapping ports to be assigned. Took a long time to
    track this down. I'm sure there's a better way to do this, but this seems
    to work for the basic testing I did.
    """
    logging.info("-------------checking for double imports-------------")
    ignore = {'posixpath.py',
              'helpers/pydev/pydevd.py',
              'importlib/_bootstrap.py',
              'importlib/_bootstrap_external.py',
              'helpers/pycharm/pytestrunner.py',
              'test/__init__.py',
              'site-packages/pytest.py',
              'python3.5/os.py',
              'python3.5/re.py'}

    files = [x.__file__ for x in list(sys.modules.values())
             if hasattr(x, "__file__")]
    dups = set([x for x in files if files.count(x) > 1])
    ignoreddups = {d for d in dups for i in ignore if i in d}
    filtereddups = dups - ignoreddups
    if filtereddups:
        error("Doubly imported files detected {}".format(filtereddups))


def setupTestLogging():
    addTraceToLogging()

    logging.basicConfig(
            level=TRACE_LOG_LEVEL,
            format='{relativeCreated:,.0f} {levelname:7s} {message:s}',
            style='{')
    console = getConsole()
    console.reinit(verbosity=console.Wordage.concise)


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

    def __init__(self, ip: str, filename: str=None, minPort=6000, maxPort=9999):
        self.ip = ip
        self.FILE = filename or os.path.join(tempfile.gettempdir(),
                                             'plenum-portmutex.{}.txt'.format(ip))
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
                    checkPortAvailable(("",port))
                    ports.append(port)
                    logger.debug("new port dispensed: {}".format(port))
                except:
                    if recurlvl < self.maxportretries:
                        logger.debug("port {} unavailable, trying again...".
                                     format(port))
                    else:
                        logger.debug("port {} unavailable, max retries {} "
                                     "reached".
                                     format(port, self.maxportretries))
                        raise
            return ports

    def getNext(self, count: int=1):
        has = [HA(self.ip, port) for port in self.get(count)]
        if len(has) == 1:
            return has[0]
        else:
            return has
