import functools
import time

import base58
import random

import pytest
from orderedset._orderedset import OrderedSet

from plenum.common.constants import DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import InvalidClientMessageException
from plenum.common.util import randomString
from plenum.server.propagator import Requests
from plenum.test.helper import sdk_random_request_objects

REQ_COUNT = 10


@pytest.fixture(scope='function', params=[0])
def replica(replica):
    replica.node.requests = Requests()
    replica.isMaster = True
    replica.node.doDynamicValidation = functools.partial(randomDynamicValidation, replica.node)
    replica.node.applyReq = lambda self, *args, **kwargs: True
    replica.stateRootHash = lambda self, *args, **kwargs: base58.b58encode(randomString(32)).decode()
    replica.txnRootHash = lambda self, *args, **kwargs: base58.b58encode(randomString(32)).decode()
    replica.node.onBatchCreated = lambda self, *args, **kwargs: True
    replica.requestQueues[DOMAIN_LEDGER_ID] = OrderedSet()
    return replica


def randomDynamicValidation(self, req):
    if random.randint(0, 1) == True:
        raise InvalidClientMessageException('aaaaaaaa',
                                             req.reqId,
                                             "not valid req")


def test_order_reqIdr(replica, looper, sdk_wallet_steward):
    _, did = sdk_wallet_steward
    reqs = sdk_random_request_objects(REQ_COUNT, identifier=did,
                                          protocol_version=CURRENT_PROTOCOL_VERSION)
    for req in reqs:
        replica.requestQueues[DOMAIN_LEDGER_ID].add(req.key)
        replica.requests.add(req)
        replica.requests.set_finalised(req)
    replica.last_accepted_pre_prepare_time = int(time.time())
    pp = replica.create3PCBatch(DOMAIN_LEDGER_ID)
    assert pp.reqIdr == [res.key for res in reqs]
