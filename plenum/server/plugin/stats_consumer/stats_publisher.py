import asyncio
from collections import deque
from enum import Enum, unique

from plenum.common.log import getlogger
from plenum.common.util import getConfig

logger = getlogger()
config = getConfig()

# MAX number of re-tries of workaround the issue with AssertionError (see below).
MAX_UNEXPECTED_CONNECTION_FAIL = 5


class StatsPublisher:
    """
    Class to send data to TCP port which runs stats collecting service
    """

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.reader = None
        self.writer = None
        self.messageBuffer = deque()
        self.loop = asyncio.get_event_loop()
        self.unexpectedConnectionFail = 0
        self._connectionSem = asyncio.Lock()

    def addMsgToBuffer(self, message):
        if len(self.messageBuffer) > config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE:
            logger.error("Message buffer is too large. Refuse to add a new message {}".format(message))
            return False

        self.messageBuffer.appendleft(message)
        return True

    def send(self, message):
        if not config.SendMonitorStats:
            return

        if not self.addMsgToBuffer(message):
            return

        if self.loop.is_running():
            self.loop.call_soon(asyncio.ensure_future, self.sendMessagesFromBuffer())
        else:
            self.loop.run_until_complete(self.sendMessagesFromBuffer())

    async def sendMessagesFromBuffer(self):
        while len(self.messageBuffer) > 0:
            message = self.messageBuffer.pop()
            await self.sendMessage(message)

    async def sendMessage(self, message):
        try:
            await self.checkConnectionAndConnect()
            await self.doSendMessage(message)
            self.unexpectedConnectionFail = 0
        except (ConnectionRefusedError, ConnectionResetError) as ex:
            self.connectionRefused(message, ex)
        except Exception as ex1:
            # this is a workaround for a problem when an input port used to establish a connection is the same as the
            # target one. An assertion error is thrown in this case and it may lead to a memory leak.
            #
            # As a workaround, we catch this exception and try to re-connect.
            #
            # Actually currently it's no needed as long as we use a port 30000
            # (which is less than default range of ports used to establish connection on Linux)
            self.connectionFailedUnexpectedly(message, ex1)

    async def checkConnectionAndConnect(self):
        if self.writer is None:
            await self._connectionSem.acquire()
            try:
                if self.writer is None:
                    self.reader, self.writer = \
                        await asyncio.streams.open_connection(self.ip, self.port, loop=self.loop)
            finally:
                self._connectionSem.release()

    async def doSendMessage(self, message):
        self.writer.write((message + '\n').encode('utf-8'))
        await self.writer.drain()

    def connectionRefused(self, message, ex):
        logger.debug("Connection refused for {}:{} while sending message: {}".
                     format(self.ip, self.port, ex))
        self.writer = None
        self.unexpectedConnectionFail = 0

    def connectionFailedUnexpectedly(self, message, ex):
        logger.debug("Can not publish stats message: {}".format(ex))
        self.writer = None
        self.unexpectedConnectionFail += 1
        # disable sending statistics at all, if the issue described above is reproduced multiple times in a row.
        if self.unexpectedConnectionFail > MAX_UNEXPECTED_CONNECTION_FAIL:
            config.SendMonitorStats = False
            self.unexpectedConnectionFail = 0


@unique
class Topic(Enum):
    ComputeLatencies = 1
    ComputeMasterThroughput = 2
    ComputeTotalTransactions = 3
    PublishMtrStats = 4
    PublishLatenciesStats = 5
    PublishConfig = 6
    PublishStartedAt = 7
    PublishViewChange = 8
    PublishTotalTransactions = 9
    PublishAllStats = 10
    IncomingEvent = 11,
    PublishNodestackStats = 12
    PublishTotalRequestsStats = 13

    def __str__(self):
        return self.name
