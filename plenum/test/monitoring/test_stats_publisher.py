import asyncio
from unittest.mock import Mock

import pytest

from plenum.config import STATS_SERVER_PORT, STATS_SERVER_IP, STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE
from plenum.server.plugin.stats_consumer.stats_publisher import StatsPublisher


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


def testReconnectEachTime(listener):
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


def testSendAllFromBuffer():
    N = 100
    statsPublisher = TestStatsPublisher()
    for i in range(N):
        statsPublisher.addMsgToBuffer("testMessage{}".format(i))

    statsPublisher.send(message="testMessage{}".format(N))

    statsPublisher.assertMessages(0, N + 1, 0)


def testSendEvenIfBufferFull():
    msg_buff_max_size = 100

    N = msg_buff_max_size + 10
    statsPublisher = TestStatsPublisher(msg_buff_max_size=msg_buff_max_size)
    for i in range(N):
        statsPublisher.addMsgToBuffer("testMessage{}".format(i))

    statsPublisher.send(message="testMessage{}".format(N))

    statsPublisher.assertMessages(
        0, msg_buff_max_size, 0)


def testUnexpectedConnectionError():
    statsPublisher = TestStatsPublisher()
    statsPublisher._checkConnectionAndConnect = Mock(
        side_effect=AssertionError("Some Error"))

    statsPublisher.send(message="testMessage")

    statsPublisher.assertMessages(0, 0, 1)


def testSendManyNoExceptions():
    N = 100
    statsPublisher = TestStatsPublisher()
    statsPublisher.localPort = None
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    assert N == len(statsPublisher.refused)


def testSendManyNoExceptionsIfDestPortFromSourceRange():
    N = 100
    # use a port that may lead to assertion error (this port may be used as an
    # input port to establish connection)
    statsPublisher = TestStatsPublisher(port=50000)
    for i in range(N):
        statsPublisher.send(message="testMessage{}".format(i))

    assert N == len(statsPublisher.refused) + \
        len(statsPublisher.unexpected) + len(statsPublisher.sent)


class TestStatsPublisher(StatsPublisher):
    def __init__(self, ip=None, port=None, msg_buff_max_size=None, localPort=None):
        super().__init__(
            ip if ip else STATS_SERVER_IP,
            port if port else STATS_SERVER_PORT,
            msg_buff_max_size if msg_buff_max_size else STATS_SERVER_MESSAGE_BUFFER_MAX_SIZE)
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
