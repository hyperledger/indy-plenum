from plenum.common.util import getMaxFailures
from plenum.test.cli.helper import checkRequest
from plenum.test.eventually import eventually
from plenum.test.helper import checkSufficientRepliesRecvd


def testClientRequest(cli, validNodeNames, cliLooper, createAllNodes):
    """
    Test client sending request and checking reply and status
    """
    operation = '{"Hello": "There"}'
    checkRequest(cli, cliLooper, operation)


