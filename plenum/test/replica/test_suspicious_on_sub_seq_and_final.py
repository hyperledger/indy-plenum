import base58
import pytest
from orderedset._orderedset import OrderedSet

from plenum.common.constants import DOMAIN_LEDGER_ID
from plenum.common.util import randomString
from plenum.server.propagator import Requests
from plenum.server.replica import PP_SUB_SEQ_NO_WRONG, PP_NOT_FINAL
from plenum.test.replica.helper import create_preprepare


@pytest.fixture(scope='function', params=[0])
def replica(replica):
    replica.node.requests = Requests()
    replica.isMaster = True
    replica.node.replica = replica
    replica.node.doDynamicValidation = lambda *args, **kwargs: True
    replica.node.applyReq = lambda self, *args, **kwargs: True
    replica.stateRootHash = lambda self, *args, **kwargs: base58.b58encode(randomString(32)).decode()
    replica.txnRootHash = lambda self, *args, **kwargs: base58.b58encode(randomString(32)).decode()
    replica.node.onBatchCreated = lambda self, *args, **kwargs: True
    replica.requestQueues[DOMAIN_LEDGER_ID] = OrderedSet()
    return replica


def test_suspicious_on_wrong_sub_seq_no(replica, sdk_wallet_steward):
    reqs, pp = create_preprepare(replica, sdk_wallet_steward, 10)
    pp.sub_seq_no = 1
    assert PP_SUB_SEQ_NO_WRONG == replica._apply_pre_prepare(pp, 'SomeNode')


def test_suspicious_on_not_final(replica, sdk_wallet_steward):
    reqs, pp = create_preprepare(replica, sdk_wallet_steward, 10)
    pp.final = False
    assert PP_NOT_FINAL == replica._apply_pre_prepare(pp, 'SomeNode')