import asyncio
from unittest.mock import Mock

import pytest

from plenum.common.config_util import getConfig
from plenum.config import STATS_SERVER_PORT, STATS_SERVER_IP
from plenum.server.plugin.stats_consumer.stats_publisher import StatsPublisher

config = getConfig()


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


@pytest.fixture
def statsServerMessageBufferMaxSizeReducer():
    originalValue = config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE
    config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE = 100
    yield
    config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE = originalValue


def testSendOneMessageNoOneListens(postingStatsEnabled):
    statsPublisher = TestStatsPublisher()
    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(0, 1, 0)
    assert ["testMessage"] == statsPublisher.refused


def testSendMultipleNoOneListens(postingStatsEnabled):
    N = 50
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    statsPublisher.assertMessages(0, N, 0)


def testSendOneMessageSomeoneListens(postingStatsEnabled, listener):
    statsPublisher = TestStatsPublisher()
    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(1, 0, 0)
    assert ["testMessage"] == statsPublisher.sent


def testSendMultipleMessagesSomeoneListens(postingStatsEnabled, listener):
    N = 50
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    statsPublisher.assertMessages(N, 0, 0)


def testReconnectEachTime(postingStatsEnabled, listener):
    # send when no one listens (send to port 30001)
    NOT_SENT_COUNT = 50
    statsPublisher = TestStatsPublisher()
    statsPublisher.port = statsPublisher.port + 1
    for i in range(NOT_SENT_COUNT):
        statsPublisher.send(message="testMessage1{}".format(i))

    # send when listens (send to port 30000)
    SENT_COUNT = 30
    statsPublisher.port = STATS_SERVER_PORT
    for i in range(SENT_COUNT):
        statsPublisher.send(message="testMessage2{}".format(i))

    # check
    statsPublisher.assertMessages(SENT_COUNT, NOT_SENT_COUNT, 0)


def testSendAllFromBuffer(postingStatsEnabled):
    N = 100
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.addMsgToBuffer("testMessage{}".format(i))

    statsPublisher.send(message="testMessage{}".format(N))

    statsPublisher.assertMessages(0, N + 1, 0)


def testSendEvenIfBufferFull(postingStatsEnabled,
                             statsServerMessageBufferMaxSizeReducer):
    N = config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE + 10
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.addMsgToBuffer("testMessage{}".format(i))

    statsPublisher.send(message="testMessage{}".format(N))

    statsPublisher.assertMessages(
        0, config.STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE, 0)


def testUnexpectedConnectionError(postingStatsEnabled):
    statsPublisher = TestStatsPublisher()
    statsPublisher._checkConnectionAndConnect = Mock(
        side_effect=AssertionError("Some Error"))

    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(0, 0, 1)


def testSendManyNoExceptions(postingStatsEnabled):
    N = 100
    statsPublisher = TestStatsPublisher()
    statsPublisher.localPort = None
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    assert N == len(statsPublisher.refused)


def testSendManyNoExceptionsIfDestPortFromSourceRange(postingStatsEnabled):
    N = 100
    # use a port that may lead to assertion error (this port may be used as an
    # input port to establish connection)
    statsPublisher = TestStatsPublisher(port=50000)
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    assert N == len(statsPublisher.refused) + \
        len(statsPublisher.unexpected) + len(statsPublisher.sent)


class TestStatsPublisher(StatsPublisher):
    def __init__(self, ip=None, port=None, localPort=None):
        super().__init__(
            ip if ip else STATS_SERVER_IP,
            port if port else STATS_SERVER_PORT)
        self.sent = []
        self.refused = []
        self.unexpected = []

    async def _doSendMessage(self, message):
        await super()._doSendMessage(message)
        self.sent.append(message)

    def _connectionRefused(self, message, ex):
        super()._connectionRefused(message, ex)
        self.refused.append(message)

    def _connectionFailedUnexpectedly(self, message, ex):
        super()._connectionFailedUnexpectedly(message, ex)
        self.unexpected.append(message)

    def assertMessages(self, expectedSent, expectedRefused,
                       expectedUnexpected):
        assert expectedSent == len(self.sent)
        assert expectedRefused == len(self.refused)
        assert expectedUnexpected == len(self.unexpected)
