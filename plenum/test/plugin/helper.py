import os

from plenum.test.test_client import TestClient

from plenum.client.wallet import Wallet


def getPluginPath(name):
    curPath = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(curPath, name)


def submitOp(wallet, client, op):
    req = wallet.signOp(op)
    client.submitReqs(req)
    return req


class App:

    def __init__(self, wallet: Wallet, client: TestClient, looper):
        self.wallet = wallet
        self.client = client
        self.looper = looper

    def submit(self, op):
        req = self.wallet.signOp(op)
        self.client.submitReqs(req)
        return req


def makeReason(common, specific):
    return '{} [caused by {}]'.format(common, specific)
