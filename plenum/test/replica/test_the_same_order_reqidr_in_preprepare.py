import functools
import time

import base58
import random

import pytest
from orderedset._orderedset import OrderedSet

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.exceptions import InvalidClientMessageException
from plenum.common.util import randomString
from plenum.server.propagator import Requests
from plenum.test.replica.helper import create_preprepare

REQ_COUNT = 10


@pytest.fixture(scope='function', params=[0])
def replica(replica):
    replica.node.requests = Requests()
    replica.isMaster = True
    replica.node.replica = replica
    replica.node.doDynamicValidation = functools.partial(randomDynamicValidation, replica.node)
    replica.node.applyReq = lambda self, *args, **kwargs: True
    replica.stateRootHash = lambda self, *args, **kwargs: base58.b58encode(randomString(32)).decode()
    replica.txnRootHash = lambda self, *args, **kwargs: base58.b58encode(randomString(32)).decode()
    replica.node.onBatchCreated = lambda self, *args, **kwargs: True
    replica.requestQueues[DOMAIN_LEDGER_ID] = OrderedSet()
    return replica


def randomDynamicValidation(self, req):
    if list(self.replica.requests.keys()).index(req.key) % 2:
        raise InvalidClientMessageException('aaaaaaaa',
                                             req.reqId,
                                             "not valid req")


def test_order_reqIdr(replica, sdk_wallet_steward):
    reqs, pp = create_preprepare(replica, sdk_wallet_steward, REQ_COUNT)
    assert pp.reqIdr == [res.key for res in reqs]
