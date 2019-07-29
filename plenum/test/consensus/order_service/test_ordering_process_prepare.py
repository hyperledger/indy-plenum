import pytest

from plenum.common.exceptions import SuspiciousNode
from plenum.common.util import updateNamedTuple
from plenum.server.suspicion_codes import Suspicions
from plenum.test.consensus.order_service.helper import _register_pp_ts
from plenum.test.helper import generate_state_root, create_prepare_from_pre_prepare

PRIMARY_NAME = "Alpha:0"
NON_PRIMARY_NAME = "Gamma:0"
OTHER_NON_PRIMARY_NAME = "Beta:0"


@pytest.fixture(scope="function")
def prepare(_pre_prepare):
    return create_prepare_from_pre_prepare(_pre_prepare)


@pytest.fixture(scope='function', params=['Primary', 'Non-Primary'])
def o(orderer_with_requests, request):
    assert orderer_with_requests.primary_name == PRIMARY_NAME
    if request == 'Primary':
        orderer_with_requests.name = PRIMARY_NAME
    else:
        orderer_with_requests.name = OTHER_NON_PRIMARY_NAME
    return orderer_with_requests


@pytest.fixture(scope="function")
def pre_prepare(o, _pre_prepare):
    _register_pp_ts(o, _pre_prepare, o.primary_name)
    return _pre_prepare


def test_process_valid_prepare(o, pre_prepare, prepare):
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    o.process_prepare(prepare, NON_PRIMARY_NAME)
    assert o.prepares.hasPrepareFrom(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_from_primary(o, prepare):
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_FRM_PRIMARY.code)):
        o.l_validatePrepare(prepare, PRIMARY_NAME)


def test_validate_duplicate_prepare(o, pre_prepare, prepare):
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    o.process_prepare(prepare, NON_PRIMARY_NAME)
    with pytest.raises(SuspiciousNode, match=str(Suspicions.DUPLICATE_PR_SENT.code)):
        o.l_validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_no_preprepare(o, prepare):
    # must sent PrePrepare before processing the Prepare
    if o.name == PRIMARY_NAME:
        with pytest.raises(SuspiciousNode, match=str(Suspicions.UNKNOWN_PR_SENT.code)):
            o.l_validatePrepare(prepare, NON_PRIMARY_NAME)
    # PrePrepare can be delayed, so just enqueue
    else:
        o.l_validatePrepare(prepare, NON_PRIMARY_NAME)
        assert (prepare, NON_PRIMARY_NAME) in o.preparesWaitingForPrePrepare[prepare.viewNo, prepare.ppSeqNo]


def test_validate_prepare_wrong_digest(o, pre_prepare, prepare):
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, digest='fake_digest')
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_DIGEST_WRONG.code)):
        o.l_validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_wrong_txn_root(o, pre_prepare, prepare):
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, txnRootHash=generate_state_root())
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_TXN_WRONG.code)):
        o.l_validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_wrong_state_root(o, pre_prepare, prepare):
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, stateRootHash=generate_state_root())
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_STATE_WRONG.code)):
        o.l_validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_wrong_audit_root(o, pre_prepare, prepare):
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, auditTxnRootHash=generate_state_root())
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_AUDIT_TXN_ROOT_HASH_WRONG.code)):
        o.l_validatePrepare(prepare, NON_PRIMARY_NAME)