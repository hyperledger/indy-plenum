import pytest

from plenum.test.eventually import eventually
from plenum.test.malicious_behaviors_client import makeClientFaulty, \
    sendsUnsignedRequest

whitelist = ['EmptySignature']


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
            assert node.isClientBlacklisted(client1.clientId)

    looper.run(eventually(chk, retryWait=1, timeout=3))
