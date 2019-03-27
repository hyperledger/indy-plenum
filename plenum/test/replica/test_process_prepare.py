import pytest

from plenum.common.exceptions import SuspiciousNode
from plenum.common.util import updateNamedTuple
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import generate_state_root

nodeCount = 7

PRIMARY_NAME = "Alpha:0"
NON_PRIMARY_NAME = "Gamma:0"
OTHER_NON_PRIMARY_NAME = "Beta:0"


@pytest.fixture(scope='function', params=[0])
def viewNo(tconf, request):
    return request.param


@pytest.fixture(scope="function",
                params=['BLS_not_None'])
def multi_sig(fake_multi_sig, request):
    if request.param == 'BLS_None':
        return None
    return fake_multi_sig


@pytest.fixture(scope='function', params=['Primary', 'Non-Primary'])
def r(replica_with_requests, request):
    assert replica_with_requests.primaryName == PRIMARY_NAME
    if request == 'Primary':
        replica_with_requests.name = PRIMARY_NAME
    else:
        replica_with_requests.name = OTHER_NON_PRIMARY_NAME
    return replica_with_requests


def test_process_valid_prepare(r, pre_prepare, prepare):
    r.processPrePrepare(pre_prepare, PRIMARY_NAME)
    r.processPrepare(prepare, NON_PRIMARY_NAME)
    assert r.prepares.hasPrepareFrom(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_from_primary(r, prepare):
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_FRM_PRIMARY.code)):
        r.validatePrepare(prepare, PRIMARY_NAME)


def test_validate_duplicate_prepare(r, pre_prepare, prepare):
    r.processPrePrepare(pre_prepare, PRIMARY_NAME)
    r.processPrepare(prepare, NON_PRIMARY_NAME)
    with pytest.raises(SuspiciousNode, match=str(Suspicions.DUPLICATE_PR_SENT.code)):
        r.validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_no_preprepare(r, prepare):
    # must sent PrePrepare before processing the Prepare
    if r.name == PRIMARY_NAME:
        with pytest.raises(SuspiciousNode, match=str(Suspicions.UNKNOWN_PR_SENT.code)):
            r.validatePrepare(prepare, NON_PRIMARY_NAME)
    # PrePrepare can be delayed, so just enqueue
    else:
        r.validatePrepare(prepare, NON_PRIMARY_NAME)
        assert (prepare, NON_PRIMARY_NAME) in r.preparesWaitingForPrePrepare[prepare.viewNo, prepare.ppSeqNo]


def test_validate_prepare_wrong_digest(r, pre_prepare, prepare):
    r.processPrePrepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, digest='fake_digest')
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_DIGEST_WRONG.code)):
        r.validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_wrong_txn_root(r, pre_prepare, prepare):
    r.processPrePrepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, txnRootHash=generate_state_root())
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_TXN_WRONG.code)):
        r.validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_wrong_state_root(r, pre_prepare, prepare):
    r.processPrePrepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, stateRootHash=generate_state_root())
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_STATE_WRONG.code)):
        r.validatePrepare(prepare, NON_PRIMARY_NAME)


def test_validate_prepare_wrong_audit_root(r, pre_prepare, prepare):
    r.processPrePrepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, auditTxnRootHash=generate_state_root())
    with pytest.raises(SuspiciousNode, match=str(Suspicions.PR_AUDIT_TXN_ROOT_HASH_WRONG.code)):
        r.validatePrepare(prepare, NON_PRIMARY_NAME)
