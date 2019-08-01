import pytest
from orderedset._orderedset import OrderedSet

from plenum.common.constants import DOMAIN_LEDGER_ID, CURRENT_PROTOCOL_VERSION, AUDIT_LEDGER_ID, POOL_LEDGER_ID
from plenum.common.messages.node_messages import PrePrepare
from plenum.common.startable import Mode
from plenum.common.timer import QueueTimer
from plenum.server.consensus.ordering_service import OrderingService, ThreePCMsgValidator
from plenum.test.consensus.order_service.helper import _register_pp_ts
from plenum.test.helper import sdk_random_request_objects, create_pre_prepare_params
from plenum.test.bls.conftest import fake_state_root_hash, fake_multi_sig, fake_multi_sig_value

@pytest.fixture()
def orderer(consensus_data, internal_bus, external_bus, name, write_manager,
            txn_roots, state_roots, bls_bft_replica):
    is_master = True  # TODO: change to a fixture
    orderer = OrderingService(data=consensus_data(name),
                              timer=QueueTimer(),
                              bus=internal_bus,
                              network=external_bus,
                              write_manager=write_manager,
                              bls_bft_replica=bls_bft_replica,
                              is_master=is_master)
    orderer._data.node_mode = Mode.participating
    orderer.primary_name = "Alpha:0"
    orderer.l_txnRootHash = lambda ledger, to_str=False: txn_roots[ledger]
    orderer.l_stateRootHash = lambda ledger, to_str=False: state_roots[ledger]
    orderer.requestQueues[DOMAIN_LEDGER_ID] = OrderedSet()
    orderer.l_revert = lambda *args, **kwargs: None
    return orderer


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


@pytest.fixture(scope="function",
                params=['BLS_not_None', 'BLS_None'])
def multi_sig(fake_multi_sig, request):
    if request.param == 'BLS_None':
        return None
    return fake_multi_sig


@pytest.fixture(scope="function")
def _pre_prepare(orderer, state_roots, txn_roots, multi_sig, fake_requests):
    params = create_pre_prepare_params(state_root=state_roots[DOMAIN_LEDGER_ID],
                                       ledger_id=DOMAIN_LEDGER_ID,
                                       txn_root=txn_roots[DOMAIN_LEDGER_ID],
                                       bls_multi_sig=multi_sig,
                                       view_no=orderer.view_no,
                                       inst_id=0,
                                       pool_state_root=state_roots[POOL_LEDGER_ID],
                                       audit_txn_root=txn_roots[AUDIT_LEDGER_ID],
                                       reqs=fake_requests,
                                       pp_seq_no=1)
    pp = PrePrepare(*params)
    return pp


@pytest.fixture(scope="function")
def pre_prepare(orderer, _pre_prepare):
    _register_pp_ts(orderer, _pre_prepare, orderer.primary_name)
    return _pre_prepare


@pytest.fixture()
def fake_requests():
    return sdk_random_request_objects(10, identifier="fake_did",
                                      protocol_version=CURRENT_PROTOCOL_VERSION)


@pytest.fixture(scope='function')
def orderer_with_requests(orderer, fake_requests):
    orderer.l_apply_pre_prepare = lambda a: (fake_requests, [], [], False)
    for req in fake_requests:
        orderer.requestQueues[DOMAIN_LEDGER_ID].add(req.key)
        orderer._requests.add(req)
        orderer._requests.set_finalised(req)

    return orderer


@pytest.fixture()
def validator(consensus_data):
    return ThreePCMsgValidator(consensus_data)


@pytest.fixture()
def primary_orderer(orderer):
    orderer.name = orderer.primary_name
    return orderer
