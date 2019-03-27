import pytest
from orderedset._orderedset import OrderedSet

from plenum.common.messages.node_messages import PrePrepare
from plenum.common.startable import Mode
from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION, AUDIT_LEDGER_ID
from plenum.common.util import get_utc_epoch
from plenum.server.propagator import Requests
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.test.conftest import getValueFromModule
from plenum.test.helper import MockTimestamp, sdk_random_request_objects, create_pre_prepare_params, \
    create_prepare_from_pre_prepare
from plenum.test.testing_utils import FakeSomething
from plenum.test.bls.conftest import fake_state_root_hash, fake_multi_sig, fake_multi_sig_value


class ReplicaFakeNode(FakeSomething):

    def __init__(self, viewNo, quorums, ledger_ids):
        node_stack = FakeSomething(
            name="fake stack",
            connecteds={"Alpha", "Beta", "Gamma", "Delta"}
        )
        super().__init__(
            name="fake node",
            ledger_ids=ledger_ids,
            viewNo=viewNo,
            quorums=quorums,
            nodestack=node_stack,
            utc_epoch=lambda *args: get_utc_epoch(),
            mode=Mode.participating,
            view_change_in_progress=False,
            requests=Requests(),
            onBatchCreated=lambda self, *args, **kwargs: True,
            applyReq=lambda self, *args, **kwargs: True
        )

    @property
    def is_synced(self) -> bool:
        return Mode.is_done_syncing(self.mode)

    @property
    def isParticipating(self) -> bool:
        return self.mode == Mode.participating


@pytest.fixture(scope='function', params=[0, 10])
def viewNo(tconf, request):
    return request.param


@pytest.fixture(scope='function')
def mock_timestamp():
    return MockTimestamp()


@pytest.fixture(scope='function')
def ledger_ids():
    return [POOL_LEDGER_ID]


@pytest.fixture(scope='function', params=[0])
def inst_id(request):
    return request.param


@pytest.fixture(scope="function")
def mock_timestamp():
    return get_utc_epoch


@pytest.fixture()
def fake_requests():
    return sdk_random_request_objects(10, identifier="fake_did",
                                      protocol_version=CURRENT_PROTOCOL_VERSION)


@pytest.fixture()
def txn_roots():
    return ["AAAgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ",
            "BBBJmfG5DYAE8ZcdTTFMiwcZaDN6CRVdSdkhBXnkYPio",
            "CCCJmfG5DYAE8ZcdTTFMiwcZaDN6CRVdSdkhBXnkYPio",
            "DDDJmfG5DYAE8ZcdTTFMiwcZaDN6CRVdSdkhBXnkYPio"]


@pytest.fixture()
def state_roots(fake_state_root_hash):
    return ["EuDgqga9DNr4bjH57Rdq6BRtvCN1PV9UX5Mpnm9gbMAZ",
            fake_state_root_hash,
            "D95JmfG5DYAE8ZcdTTFMiwcZaDN6CRVdSdkhBXnkYPio",
            None]


@pytest.fixture(scope='function')
def replica(tconf, viewNo, inst_id, ledger_ids, mock_timestamp, fake_requests, txn_roots, state_roots, request):
    node = ReplicaFakeNode(viewNo=viewNo,
                           quorums=Quorums(getValueFromModule(request, 'nodeCount', default=4)),
                           ledger_ids=ledger_ids)
    bls_bft_replica = FakeSomething(
        gc=lambda *args: None,
        update_pre_prepare=lambda params, l_id: params,
        validate_pre_prepare=lambda a, b: None,
        validate_prepare=lambda a, b: None,
        update_prepare=lambda a, b: a,
        process_prepare=lambda a, b: None,
        process_pre_prepare=lambda a, b: None,
        process_order =lambda *args: None
    )
    replica = Replica(
        node, instId=inst_id, isMaster=inst_id == 0,
        config=tconf, bls_bft_replica=bls_bft_replica,
        get_current_time=mock_timestamp,
        get_time_for_3pc_batch=mock_timestamp
    )
    ReplicaFakeNode.master_last_ordered_3PC = replica.last_ordered_3pc

    replica.last_accepted_pre_prepare_time = replica.get_time_for_3pc_batch()
    replica.revert = lambda ledgerId, stateRootHash, reqCount: None
    replica.primaryName = "Alpha:{}".format(replica.instId)
    replica.primaryNames[replica.viewNo] = replica.primaryName

    replica.txnRootHash = lambda ledger, to_str=False: txn_roots[ledger]
    replica.stateRootHash = lambda ledger, to_str=False: state_roots[ledger]

    replica.requestQueues[DOMAIN_LEDGER_ID] = OrderedSet()

    def reportSuspiciousNodeEx(ex):
        assert False, ex

    replica.node.reportSuspiciousNodeEx = reportSuspiciousNodeEx

    return replica


@pytest.fixture(scope='function')
def replica_with_requests(replica, fake_requests):
    replica._apply_pre_prepare = lambda a: (fake_requests, [], [])
    for req in fake_requests:
        replica.requestQueues[DOMAIN_LEDGER_ID].add(req.key)
        replica.requests.add(req)
        replica.requests.set_finalised(req)

    return replica


@pytest.fixture(scope="function",
                params=['BLS_not_None', 'BLS_None'])
def multi_sig(fake_multi_sig, request):
    if request.param == 'BLS_None':
        return None
    return fake_multi_sig


@pytest.fixture(scope="function")
def pre_prepare(replica, state_roots, txn_roots, multi_sig, fake_requests):
    params = create_pre_prepare_params(state_root=state_roots[DOMAIN_LEDGER_ID],
                                       ledger_id=DOMAIN_LEDGER_ID,
                                       txn_root=txn_roots[DOMAIN_LEDGER_ID],
                                       bls_multi_sig=multi_sig,
                                       view_no=replica.viewNo,
                                       inst_id=replica.instId,
                                       pool_state_root=state_roots[POOL_LEDGER_ID],
                                       audit_txn_root=txn_roots[AUDIT_LEDGER_ID],
                                       reqs=fake_requests)
    pp = PrePrepare(*params)
    return pp


@pytest.fixture(scope="function")
def prepare(pre_prepare):
    return create_prepare_from_pre_prepare(pre_prepare)