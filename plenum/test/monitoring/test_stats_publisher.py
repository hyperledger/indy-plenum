import asyncio
from unittest.mock import Mock

import pytest

from plenum.common.util import getConfig
from plenum.config import STATS_SERVER_PORT, STATS_SERVER_IP
from plenum.server.plugin.stats_consumer.stats_publisher import StatsPublisher, MAX_UNEXPECTED_CONNECTION_FAIL


@pytest.fixture(scope="function")
def listener():
    def _acceptClient(clientReader, clientWriter):
        pass

    loop = asyncio.get_event_loop()
    server = loop.run_until_complete(
        asyncio.start_server(_acceptClient,
                             host=STATS_SERVER_IP, port=STATS_SERVER_PORT,
                             loop=loop))
    yield server
    server.close()
    loop.run_until_complete(server.wait_closed())


@pytest.fixture(scope="function")
def configSendingStats():
    config = getConfig()
    config.SendMonitorStats = True
    yield config
    config.SendMonitorStats = True


def testSendOneMessageNoOneListens():
    statsPublisher = TestStatsPublisher()
    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(0, 1, 0)
    assert ["testMessage"] == statsPublisher.refused


def testSendMultipleNoOneListens():
    N = 50
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    statsPublisher.assertMessages(0, N, 0)


def testSendOneMessageSomeoneListens(listener):
    statsPublisher = TestStatsPublisher()
    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(1, 0, 0)
    assert ["testMessage"] == statsPublisher.sent


def testSendMultipleMessagesSomeoneListens(listener):
    N = 50
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    statsPublisher.assertMessages(N, 0, 0)


def testSendAllFromBuffer():
    N = 100
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.addMsgToBuffer("testMessage{}".format(i))

    statsPublisher.send(message="testMessage{}".format(N))

    statsPublisher.assertMessages(0, N + 1, 0)


def testUnexpectedConnectionError():
    statsPublisher = TestStatsPublisher()
    statsPublisher.checkConnectionAndConnect = Mock(side_effect=AssertionError("Some Error"))

    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(0, 0, 1)


def testMultipleUnexpectedConnectionErrorsStopSending(configSendingStats):
    statsPublisher = TestStatsPublisher()
    statsPublisher.checkConnectionAndConnect = Mock(side_effect=AssertionError("Some Error"))

    for i in range(MAX_UNEXPECTED_CONNECTION_FAIL):
        statsPublisher.send(message="testMessage{}".format(i))
        statsPublisher.assertMessages(0, 0, i + 1)
        assert True == configSendingStats.SendMonitorStats

    statsPublisher.send(message="testLastMessage1")
    statsPublisher.assertMessages(0, 0, MAX_UNEXPECTED_CONNECTION_FAIL + 1)
    assert False == configSendingStats.SendMonitorStats

    statsPublisher.send(message="testLastMessage2")
    statsPublisher.assertMessages(0, 0, MAX_UNEXPECTED_CONNECTION_FAIL + 1)
    assert False == configSendingStats.SendMonitorStats


def testSendManyNoExceptions():
    N = 50000
    # use a port that may lead to assertion error (this port may be used as an input port to establish connection)
    statsPublisher = TestStatsPublisher(port=50000)
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    assert N == len(statsPublisher.refused) + len(statsPublisher.unexpected) + len(statsPublisher.sent)


class TestStatsPublisher(StatsPublisher):
    def __init__(self, ip=None, port=None):
        super().__init__(ip if ip else STATS_SERVER_IP, port if port else STATS_SERVER_PORT)
        self.sent = []
        self.refused = []
        self.unexpected = []

    async def doSendMessage(self, message):
        self.sent.append(message)
        await super().doSendMessage(message)

    def connectionRefused(self, message, ex):
        self.refused.append(message)
        super().connectionRefused(message, ex)

    def connectionFailedUnexpectedly(self, message, ex):
        self.unexpected.append(message)
        super().connectionFailedUnexpectedly(message, ex)

    def assertMessages(self, expectedSent, expectedRefused, expectedUnexpected):
        assert expectedSent == len(self.sent)
        assert expectedRefused == len(self.refused)
        assert expectedUnexpected == len(self.unexpected)
