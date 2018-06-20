import os

from plenum.test.test_client import TestClient

from plenum.client.wallet import Wallet


def getPluginPath(name):
    curPath = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(curPath, name)


def makeReason(common, specific):
    return '{} [caused by {}]'.format(common, specific)
