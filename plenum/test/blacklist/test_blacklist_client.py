import pytest

from plenum.test.eventually import eventually
from plenum.test.malicious_behaviors_client import makeClientFaulty, \
    sendsUnsignedRequest


@pytest.fixture(scope="module")
def setup(client1):
    makeClientFaulty(client1, sendsUnsignedRequest)


# noinspection PyIncorrectDocstring,PyUnusedLocal,PyShadowingNames
def testBlacklistClient(setup, looper, nodeSet, up, client1, sent1):
    """
    Client should be blacklisted by node on sending an unsigned request
    """

    # Every node should blacklist the client
    def chk():
        for node in nodeSet:
            assert not node.isClientBlacklisted(client1.name)

    # TODO: Add test for client sending invalid signature to node not being
    #  blacklisted.

    looper.run(eventually(chk, retryWait=1, timeout=3))
