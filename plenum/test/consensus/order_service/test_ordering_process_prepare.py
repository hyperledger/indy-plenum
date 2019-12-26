from unittest.mock import Mock

import pytest

from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.internal_messages import RaisedSuspicion
from plenum.common.util import updateNamedTuple
from plenum.server.suspicion_codes import Suspicions
from plenum.test.consensus.order_service.helper import _register_pp_ts, check_suspicious
from plenum.test.helper import generate_state_root, create_prepare_from_pre_prepare

PRIMARY_NAME = "Alpha:0"
NON_PRIMARY_NAME = "Gamma:0"
OTHER_NON_PRIMARY_NAME = "Beta:0"


@pytest.fixture(scope="function")
def prepare(_pre_prepare):
    return create_prepare_from_pre_prepare(_pre_prepare)


@pytest.fixture(scope='function', params=['Primary', 'Non-Primary'])
def o(orderer_with_requests, request):
    orderer_with_requests._data.primary_name == PRIMARY_NAME
    if request.param == 'Primary':
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
    handler = Mock()
    o._bus.subscribe(RaisedSuspicion, handler)
    o._validate_prepare(prepare, PRIMARY_NAME)
    check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                              ex=SuspiciousNode(PRIMARY_NAME,
                                                                Suspicions.PR_FRM_PRIMARY,
                                                                prepare)))


def test_validate_duplicate_prepare(o, pre_prepare, prepare):
    handler = Mock()
    o._bus.subscribe(RaisedSuspicion, handler)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    o.process_prepare(prepare, NON_PRIMARY_NAME)
    o._validate_prepare(prepare, NON_PRIMARY_NAME)
    check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                              ex=SuspiciousNode(NON_PRIMARY_NAME,
                                                                Suspicions.DUPLICATE_PR_SENT,
                                                                prepare)))


def test_validate_prepare_no_preprepare_before_prep_cert(o, prepare):
    o._data.prev_view_prepare_cert = prepare.ppSeqNo
    # must stash Prepare regardless if this is Primary if Prepare is for batches in re-ordering phase (less than prep_cert)
    o._validate_prepare(prepare, NON_PRIMARY_NAME)
    assert (prepare, NON_PRIMARY_NAME) in o.preparesWaitingForPrePrepare[prepare.viewNo, prepare.ppSeqNo]


def test_validate_prepare_no_preprepare_after_prep_cert(o, prepare):
    o._data.prev_view_prepare_cert = prepare.ppSeqNo - 1
    # must sent PrePrepare before processing the Prepare
    if o.name == PRIMARY_NAME:
        handler = Mock()
        o._bus.subscribe(RaisedSuspicion, handler)
        o._validate_prepare(prepare, NON_PRIMARY_NAME)
        check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                                  ex=SuspiciousNode(NON_PRIMARY_NAME,
                                                                    Suspicions.UNKNOWN_PR_SENT,
                                                                    prepare)))
    # PrePrepare can be delayed, so just enqueue
    else:
        o._validate_prepare(prepare, NON_PRIMARY_NAME)
        assert (prepare, NON_PRIMARY_NAME) in o.preparesWaitingForPrePrepare[prepare.viewNo, prepare.ppSeqNo]


def test_validate_prepare_wrong_digest(o, pre_prepare, prepare):
    handler = Mock()
    o._bus.subscribe(RaisedSuspicion, handler)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, digest='fake_digest')
    o._validate_prepare(prepare, NON_PRIMARY_NAME)
    check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                              ex=SuspiciousNode(NON_PRIMARY_NAME,
                                                                Suspicions.PR_DIGEST_WRONG,
                                                                prepare)))


def test_validate_prepare_wrong_txn_root(o, pre_prepare, prepare):
    handler = Mock()
    o._bus.subscribe(RaisedSuspicion, handler)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, txnRootHash=generate_state_root())
    o._validate_prepare(prepare, NON_PRIMARY_NAME)
    check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                              ex=SuspiciousNode(NON_PRIMARY_NAME,
                                                                Suspicions.PR_TXN_WRONG,
                                                                prepare)))


def test_validate_prepare_wrong_state_root(o, pre_prepare, prepare):
    handler = Mock()
    o._bus.subscribe(RaisedSuspicion, handler)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, stateRootHash=generate_state_root())
    o._validate_prepare(prepare, NON_PRIMARY_NAME)
    check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                              ex=SuspiciousNode(NON_PRIMARY_NAME,
                                                                Suspicions.PR_STATE_WRONG,
                                                                prepare)))


def test_validate_prepare_wrong_audit_root(o, pre_prepare, prepare):
    handler = Mock()
    o._bus.subscribe(RaisedSuspicion, handler)
    o.process_preprepare(pre_prepare, PRIMARY_NAME)
    prepare = updateNamedTuple(prepare, auditTxnRootHash=generate_state_root())
    o._validate_prepare(prepare, NON_PRIMARY_NAME)
    check_suspicious(handler, RaisedSuspicion(inst_id=o._data.inst_id,
                                              ex=SuspiciousNode(NON_PRIMARY_NAME,
                                                                Suspicions.PR_AUDIT_TXN_ROOT_HASH_WRONG,
                                                                prepare)))
