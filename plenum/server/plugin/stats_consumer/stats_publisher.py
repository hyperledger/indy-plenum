import asyncio

from enum import Enum, unique


class StatsPublisher:
    """
    Class to send data to TCP port which runs stats collecting service
    """

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.reader = None
        self.writer = None

    async def sendMessage(self, message):
        loop = asyncio.get_event_loop()
        self.reader, self.writer = await asyncio.streams.open_connection(
            self.ip, self.port, loop=loop)
        self.writer.write((message + '\n').encode('utf-8'))

    async def closeWriter(self):
        if self.writer is not None:
            self.writer.close()

        await asyncio.sleep(0.1)

    def send(self, message):
        async def run():
            await self.sendMessage(message=message)
            await asyncio.sleep(0.01)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(run())
        loop.run_until_complete(self.closeWriter())


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
    IncomingEvent = 11

    def __str__(self):
        return self.name
