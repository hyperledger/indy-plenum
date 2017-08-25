from plenum.test.cli.helper import checkRequest


def testClientRequest(cli, validNodeNames, createAllNodes):
    """
    Test client sending request and checking reply and status
    """
    operation = '{"Hello": "There", "type": "greeting"}'
    checkRequest(cli, operation)
