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

    async def checkConectionAndConnect(self):
        if self.writer is None:
            await self._connectionSem.acquire()
            try:
                if self.writer is None:
                    self.reader, self.writer = \
                        await asyncio.streams.open_connection(self.ip, self.port, loop=self.loop)
            finally:
                self._connectionSem.release()


    async def sendMessagesFromBuffer(self):
        while len(self.messageBuffer) > 0:
            message = self.messageBuffer.pop()
            await self.sendMessage(message)

    async def sendMessage(self, message):
        try:
            await self.checkConectionAndConnect()
            await self.doSendMessage(message)
            self.unexpectedConnectionFail = 0
        except (ConnectionRefusedError, ConnectionResetError) as ex:
            logger.debug("Connection refused for {}:{} while sending message: {}".
                         format(self.ip, self.port, ex))
            self.writer = None
            self.unexpectedConnectionFail = 0
        except Exception as ex1:
            # this is a workaround for an extreme memory leak
            # sometimes the drain() method above is called, that is checkConectionAndConnect() doesn't throw
            # any exceptions, although no one listens on the port, so exception must be thrown and drain must not be called.
            # It leads to an AssertionException.
            # As a workaround, we catch this exception and try to re-connect (usually it helps).
            # TODO: find out what is the exact reason of such behaviour: a race condition in asyncio and streams, or incorrect usage of API?
            # we try to re-connect as a workaround
            logger.debug("Can not publish stats message: {}".format(ex1))
            self.writer = None
            self.unexpectedConnectionFail += 1
            # disable sending statistics at all, if the issue described above is reproduced multiple times in a row.
            if self.unexpectedConnectionFail > MAX_UNEXPECTED_CONNECTION_FAIL:
                config.SendMonitorStats = False
                self.unexpectedConnectionFail = 0

    async def doSendMessage(self, message):
        self.writer.write((message + '\n').encode('utf-8'))
        await self.writer.drain()

    def send(self, message):
        if len(self.messageBuffer) > config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE:
            logger.error("Message buffer is too large. Refuse to add a new message {}".format(message))
            return

        self.messageBuffer.appendleft(message)
        asyncio.ensure_future(self.sendMessagesFromBuffer())

        if not self.loop.is_running():
            self.loop.run_forever()


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
