import logging
import os
import sys

import fcntl
from ioflo.base.consoling import getConsole

from zeno.common.stacked import HA
from zeno.common.util import error, addTraceToLogging, TRACE_LOG_LEVEL, \
    checkPortAvailable


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
    console.reinit(verbosity=console.Wordage.terse)


class adict(dict):
    marker = object()

    def __init__(self, **kwargs):
        super().__init__()
        for key in kwargs:
            self.__setitem__(key, kwargs[key])

    def __setitem__(self, key, value):
        if isinstance(value, dict) and not isinstance(value, adict):
            value = adict(**value)
        super(adict, self).__setitem__(key, value)

    def __getitem__(self, key):
        found = self.get(key, adict.marker)
        if found is adict.marker:
            found = adict()
            super(adict, self).__setitem__(key, found)
        return found

    __setattr__ = __setitem__
    __getattr__ = __getitem__


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
    def __init__(self, ip: str):
        self.ip = ip
        self.FILE = "portmutex3.{}.txt".format(ip)
        self.minPort = 6000
        self.maxPort = 9999
        self.initFile()

    def initFile(self):
        if not os.path.exists(self.FILE):
            with open(self.FILE, "w") as file:
                file.write(str(self.minPort))

    def get(self, count: int=1, readOnly: bool=False):
        with open(self.FILE, "r+") as file:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)
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
                if checkPortAvailable(HA(self.ip, port)):
                    ports.append(port)
                    print("new port dispensed: {}".format(port))
                else:
                    print("new port not available: {}".format(port))
            return ports

    def getNext(self, count: int=1):
        has = [HA(self.ip, port) for port in self.get(count)]
        if len(has) == 1:
            return has[0]
        else:
            return has
