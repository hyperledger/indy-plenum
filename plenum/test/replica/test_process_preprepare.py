from copy import copy

import pytest

from plenum.common.constants import DOMAIN_LEDGER_ID, POOL_LEDGER_ID, AUDIT_LEDGER_ID
from plenum.common.exceptions import SuspiciousNode
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.types import f
from plenum.server.replica import PP_SUB_SEQ_NO_WRONG, PP_NOT_FINAL
from plenum.server.suspicion_codes import Suspicions
from plenum.test.helper import create_pre_prepare_params
from plenum.test.replica.helper import expect_suspicious
from plenum.test.testing_utils import FakeSomething
from stp_zmq.zstack import ZStack

nodeCount = 4


@pytest.fixture(scope='function')
def mock_schema_pool_state_root():
    old_schema = copy(PrePrepare.schema)
    PrePrepare.schema = tuple(y for y in PrePrepare.schema if y[0] != f.POOL_STATE_ROOT_HASH.nm)
    yield PrePrepare.schema
    PrePrepare.schema = old_schema


@pytest.fixture(scope='function')
def mock_schema_audit_txn_root():
    old_schema = copy(PrePrepare.schema)
    PrePrepare.schema = tuple(y for y in PrePrepare.schema if y[0] != f.AUDIT_TXN_ROOT_HASH.nm)
    yield PrePrepare.schema
    PrePrepare.schema = old_schema


def test_process_pre_prepare_validation(replica_with_requests,
                                        pre_prepare):
    pre_prepare.frm = replica_with_requests.primaryName
    replica_with_requests.processPrePrepare(pre_prepare)


def test_process_pre_prepare_validation_old_schema_no_pool(replica_with_requests,
                                                           pre_prepare,
                                                           mock_schema_pool_state_root):
    serialized_pp = ZStack.serializeMsg(pre_prepare)
    deserialized_pp = ZStack.deserializeMsg(serialized_pp)
    assert f.POOL_STATE_ROOT_HASH.nm not in PrePrepare.schema

    pp = PrePrepare(**deserialized_pp)
    pp.frm = replica_with_requests.primaryName
    replica_with_requests.processPrePrepare(pp)


def test_process_pre_prepare_validation_old_schema_no_audit(replica_with_requests,
                                                            pre_prepare,
                                                            mock_schema_audit_txn_root):
    serialized_pp = ZStack.serializeMsg(pre_prepare)
    deserialized_pp = ZStack.deserializeMsg(serialized_pp)
    assert f.AUDIT_TXN_ROOT_HASH.nm not in PrePrepare.schema

    pp = PrePrepare(**deserialized_pp)
    pp.frm = replica_with_requests.primaryName
    replica_with_requests.processPrePrepare(pp)


def test_process_pre_prepare_with_incorrect_pool_state_root(replica_with_requests,
                                                            state_roots, txn_roots, multi_sig, fake_requests):
    expect_suspicious(replica_with_requests, Suspicions.PPR_POOL_STATE_ROOT_HASH_WRONG.code)

    pre_prepare_params = create_pre_prepare_params(state_root=state_roots[DOMAIN_LEDGER_ID],
                                                   ledger_id=DOMAIN_LEDGER_ID,
                                                   txn_root=txn_roots[DOMAIN_LEDGER_ID],
                                                   bls_multi_sig=multi_sig,
                                                   view_no=replica_with_requests.viewNo,
                                                   inst_id=replica_with_requests.instId,
                                                   # INVALID!
                                                   pool_state_root="HSai3sMHKeAva4gWMabDrm1yNhezvPHfXnGyHf2ex1L4",
                                                   audit_txn_root=txn_roots[AUDIT_LEDGER_ID],
                                                   reqs=fake_requests)
    pre_prepare = PrePrepare(*pre_prepare_params)
    pre_prepare.frm = replica_with_requests.primaryName

    with pytest.raises(SuspiciousNode):
        replica_with_requests.processPrePrepare(pre_prepare)


def test_process_pre_prepare_with_incorrect_audit_txn_root(replica_with_requests,
                                                           state_roots, txn_roots, multi_sig, fake_requests):
    expect_suspicious(replica_with_requests, Suspicions.PPR_AUDIT_TXN_ROOT_HASH_WRONG.code)

    pre_prepare_params = create_pre_prepare_params(state_root=state_roots[DOMAIN_LEDGER_ID],
                                                   ledger_id=DOMAIN_LEDGER_ID,
                                                   txn_root=txn_roots[DOMAIN_LEDGER_ID],
                                                   bls_multi_sig=multi_sig,
                                                   view_no=replica_with_requests.viewNo,
                                                   inst_id=replica_with_requests.instId,
                                                   pool_state_root=state_roots[POOL_LEDGER_ID],
                                                   # INVALID!
                                                   audit_txn_root="HSai3sMHKeAva4gWMabDrm1yNhezvPHfXnGyHf2ex1L4",
                                                   reqs=fake_requests)
    pre_prepare = PrePrepare(*pre_prepare_params)
    pre_prepare.frm = replica_with_requests.primaryName

    with pytest.raises(SuspiciousNode):
        replica_with_requests.processPrePrepare(pre_prepare)


def test_process_pre_prepare_with_not_final_request(replica, pre_prepare):
    replica.node.seqNoDB = FakeSomething(get=lambda req: (None, None))
    replica.nonFinalisedReqs = lambda a: set(pre_prepare.reqIdr)

    def request_propagates(reqs):
        assert reqs == set(pre_prepare.reqIdr)

    replica.node.request_propagates = request_propagates

    pre_prepare.frm = replica.primaryName
    replica.processPrePrepare(pre_prepare)
    assert (pre_prepare, replica.primaryName, set(pre_prepare.reqIdr)) in replica.prePreparesPendingFinReqs


def test_process_pre_prepare_with_ordered_request(replica, pre_prepare):
    expect_suspicious(replica, Suspicions.PPR_WITH_ORDERED_REQUEST.code)

    replica.node.seqNoDB = FakeSomething(get=lambda req: (1, 1))
    replica.nonFinalisedReqs = lambda a: pre_prepare.reqIdr

    def request_propagates(reqs):
        assert False, "Requested propagates for: {}".format(reqs)

    replica.node.request_propagates = request_propagates

    with pytest.raises(SuspiciousNode):
        pre_prepare.frm = replica.primaryName
        replica.processPrePrepare(pre_prepare)
    assert (pre_prepare, replica.primaryName, set(pre_prepare.reqIdr)) not in replica.prePreparesPendingFinReqs


def test_suspicious_on_wrong_sub_seq_no(replica_with_requests, pre_prepare):
    pre_prepare.sub_seq_no = 1
    pre_prepare.frm = replica_with_requests.primaryName
    # TODO INDY-1983 fix
    assert PP_SUB_SEQ_NO_WRONG == replica_with_requests._process_valid_preprepare(pre_prepare)


def test_suspicious_on_not_final(replica_with_requests, pre_prepare):
    pre_prepare.final = False
    pre_prepare.frm = replica_with_requests.primaryName
    # TODO INDY-1983 fix
    assert PP_NOT_FINAL == replica_with_requests._process_valid_preprepare(pre_prepare)
