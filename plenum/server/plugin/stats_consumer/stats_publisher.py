import asyncio
from collections import deque
from enum import Enum, unique

from stp_core.common.log import getlogger

logger = getlogger()


class StatsPublisher:
    """
    Class to send data to TCP port which runs stats collecting service
    """

    def __init__(self, destIp, destPort, msg_buff_max_size):
        self.ip = destIp
        self.port = destPort
        self.msg_buff_max_size = msg_buff_max_size
        self._reader = None
        self._writer = None
        self._messageBuffer = deque()
        self._loop = asyncio.get_event_loop()
        self._connectionSem = asyncio.Lock()

    def addMsgToBuffer(self, message):
        if len(self._messageBuffer) >= self.msg_buff_max_size:
            logger.error("Message buffer is too large. Refuse to add a new message {}".format(message))
            return False

        self._messageBuffer.appendleft(message)
        return True

    def send(self, message):
        self.addMsgToBuffer(message)

        if self._loop.is_running():
            self._loop.call_soon(asyncio.ensure_future,
                                 self.sendMessagesFromBuffer())
        else:
            self._loop.run_until_complete(self.sendMessagesFromBuffer())

    async def sendMessagesFromBuffer(self):
        while self._messageBuffer:
            message = self._messageBuffer.pop()
            await self.sendMessage(message)

    async def sendMessage(self, message):
        try:
            await self._checkConnectionAndConnect()
            await self._doSendMessage(message)
        except (ConnectionRefusedError, ConnectionResetError) as ex:
            self._connectionRefused(message, ex)
        except Exception as ex1:
            self._connectionFailedUnexpectedly(message, ex1)

    async def _checkConnectionAndConnect(self):
        if self._writer is None:
            await self._connectionSem.acquire()
            try:
                if self._writer is None:
                    self._reader, self._writer = \
                        await asyncio.streams.open_connection(host=self.ip, port=self.port, loop=self._loop)
            finally:
                self._connectionSem.release()

    async def _doSendMessage(self, message):
        self._writer.write((message + '\n').encode('utf-8'))
        await self._writer.drain()

    def _connectionRefused(self, message, ex):
        logger.display("Connection refused for {}:{} while sending message: {}".
                       format(self.ip, self.port, ex))
        self._writer = None

    def _connectionFailedUnexpectedly(self, message, ex):
        # this is a workaround for a problem when an input port used to establish a connection is the same as the
        # target one. An assertion error is thrown in this case and it may lead to a memory leak.
        #
        # As a workaround, we catch this exception and try to re-connect.
        #
        # Actually currently it's no needed as long as we use a port 30000 as destination and specified port != 30000 as source
        # (which is less than default range of ports used to establish connection on Linux)
        logger.debug("Cannot publish stats message: {}".format(ex))
        self._writer = None


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
    PublishNodeStats = 14
    PublishSystemStats = 15

    def __str__(self):
        return self.name
