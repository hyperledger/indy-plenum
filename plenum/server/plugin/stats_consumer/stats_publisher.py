import asyncio

from enum import Enum, unique

from plenum.common.log import getlogger
from plenum.common.util import getConfig
from collections import deque

logger = getlogger()
config= getConfig()


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

    async def checkConectionAndConnect(self):
        if self.writer is None:
            self.reader, self.writer = await asyncio.streams.open_connection(
                self.ip, self.port, loop=self.loop)

    async def sendMessagesFromBuffer(self):
        while len(self.messageBuffer) > 0:
            message = self.messageBuffer.pop()
            await self.sendMessage(message)

    async def sendMessage(self, message):
        try:
            await self.checkConectionAndConnect()
            self.writer.write((message + '\n').encode('utf-8'))
            await self.writer.drain()
        except (ConnectionRefusedError, ConnectionResetError) as ex:
            logger.debug("Connection refused for {}:{} while sending message: {}".
                        format(self.ip, self.port, ex))
            self.writer = None
        except Exception as ex1:
            logger.debug("Can not publish stats message: {}".format(ex1))
            self.writer = None


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
