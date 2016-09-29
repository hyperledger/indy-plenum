import os
import tempfile


import portalocker
from plenum.cli.constants import UTIL_GRAMS_SIMPLE_CMD_FORMATTED_REG_EX, \
    UTIL_GRAMS_COMMAND_HELP_FORMATTED_REG_EX, \
    NODE_GRAMS_NODE_COMMAND_FORMATTED_REG_EX, \
    CLIENT_GRAMS_CLIENT_COMMAND_FORMATTED_REG_EX, \
    CLIENT_GRAMS_CLIENT_SEND_FORMATTED_REG_EX, \
    CLIENT_GRAMS_CLIENT_SHOW_FORMATTED_REG_EX, \
    CLIENT_GRAMS_ADD_KEY_FORMATTED_REG_EX, \
    CLIENT_GRAMS_NEW_KEYPAIR_FORMATTED_REG_EX, \
    CLIENT_GRAMS_LIST_IDS_FORMATTED_REG_EX, \
    CLIENT_GRAMS_BECOME_FORMATTED_REG_EX, \
    CLIENT_GRAMS_USE_KEYPAIR_FORMATTED_REG_EX, \
    UTIL_GRAMS_COMMAND_LIST_FORMATTED_REG_EX, \
    NODE_GRAMS_LOAD_PLUGINS_FORMATTED_REG_EX, \
    CLIENT_GRAMS_ADD_GENESIS_TXN_FORMATTED_REG_EX, \
    CLIENT_GRAMS_CREATE_GENESIS_TXN_FILE_FORMATTED_REG_EX, \
    UTIL_GRAMS_COMMAND_PROMPT_FORMATTED_REG_EX, \
    CLIENT_GRAMS_NEW_KEYRING_FORMATTED_REG_EX, \
    CLIENT_GRAMS_RENAME_KEYRING_FORMATTED_REG_EX, \
    CLIENT_GRAMS_USE_KEYRING_FORMATTED_REG_EX
from plenum.common.types import HA
from plenum.common.util import checkPortAvailable, getlogger

logger = getlogger()

def getUtilGrams():
    return [
        UTIL_GRAMS_SIMPLE_CMD_FORMATTED_REG_EX,
        UTIL_GRAMS_COMMAND_HELP_FORMATTED_REG_EX,
        UTIL_GRAMS_COMMAND_PROMPT_FORMATTED_REG_EX,
        UTIL_GRAMS_COMMAND_LIST_FORMATTED_REG_EX
    ]


def getNodeGrams():
    return [
        NODE_GRAMS_NODE_COMMAND_FORMATTED_REG_EX,
        NODE_GRAMS_LOAD_PLUGINS_FORMATTED_REG_EX,
    ]


def getClientGrams():
    return [
        CLIENT_GRAMS_CLIENT_COMMAND_FORMATTED_REG_EX,
        CLIENT_GRAMS_CLIENT_SEND_FORMATTED_REG_EX,
        CLIENT_GRAMS_CLIENT_SHOW_FORMATTED_REG_EX,
        CLIENT_GRAMS_ADD_KEY_FORMATTED_REG_EX,
        CLIENT_GRAMS_NEW_KEYPAIR_FORMATTED_REG_EX,
        CLIENT_GRAMS_NEW_KEYRING_FORMATTED_REG_EX,
        CLIENT_GRAMS_RENAME_KEYRING_FORMATTED_REG_EX,
        CLIENT_GRAMS_LIST_IDS_FORMATTED_REG_EX,
        CLIENT_GRAMS_BECOME_FORMATTED_REG_EX,
        CLIENT_GRAMS_ADD_GENESIS_TXN_FORMATTED_REG_EX,
        CLIENT_GRAMS_CREATE_GENESIS_TXN_FILE_FORMATTED_REG_EX,
        CLIENT_GRAMS_USE_KEYPAIR_FORMATTED_REG_EX,
        CLIENT_GRAMS_USE_KEYRING_FORMATTED_REG_EX
    ]


def getAllGrams(*grams):
    # Adding "|" to `utilGrams` and `nodeGrams` so they can be combined
    allGrams = []
    for gram in grams[:-1]:
        allGrams += gram
        allGrams[-1] += " |"
    return allGrams + grams[-1]


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


genHa = PortDispenser("127.0.0.1").getNext