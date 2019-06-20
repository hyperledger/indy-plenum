import json
from enum import Enum
import pytest

from plenum.common.types import f
from plenum.common.constants import AML
from plenum.common.util import get_utc_epoch
from plenum.common.request import SafeRequest
from plenum.common.exceptions import (
    InvalidClientTaaAcceptanceError, RequestRejectedException
)

from plenum.test.helper import sdk_send_and_check
from plenum.test.node_catchup.helper import ensure_all_nodes_have_same_data
from .helper import (
    build_nym_request, build_node_request,
    add_taa_acceptance as _add_taa_acceptance,
    sign_request_dict
)


class RequestType(Enum):
    Domain = 0
    Pool = 1


class ValidationType(Enum):
    FuncApi = 0
    TxnApi = 1


@pytest.fixture(scope='module')
def activate_taa(
    set_txn_author_agreement_aml, set_txn_author_agreement,
    sdk_wallet_trustee, sdk_wallet_new_steward, sdk_wallet_client
):
    return set_txn_author_agreement()


@pytest.fixture(scope='module')
def latest_taa(get_txn_author_agreement):
    return get_txn_author_agreement()


@pytest.fixture(scope="module")
def node_validator(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture(scope='module')
def validate_taa_acceptance_func_api(node_validator):
    def wrapped(signed_req_dict):
        signed_req_obj = SafeRequest(**signed_req_dict)
        node_validator.validateTaaAcceptance(
            signed_req_obj,
            node_validator.master_replica.get_time_for_3pc_batch()
        )
    return wrapped


@pytest.fixture(scope='module')
def validate_taa_acceptance_txn_api(looper, txnPoolNodeSet, sdk_pool_handle):
    def wrapped(signed_req_dict):
        signed_req_json = json.dumps(signed_req_dict)
        sdk_send_and_check([signed_req_json], looper, txnPoolNodeSet, sdk_pool_handle)[0]
        ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    return wrapped


# parametrization spec for tests
def pytest_generate_tests(metafunc):
    if 'request_type' in metafunc.fixturenames:
        # TODO in more recent versions of pytest it might be better done
        # using markers from metafunc.definition
        request_types = [RequestType.Domain]
        if 'all_request_types' in metafunc.fixturenames:
            request_types = RequestType
        elif 'pool_request' in metafunc.fixturenames:
            request_types = [RequestType.Pool]

        metafunc.parametrize(
            'request_type', request_types, ids=lambda x: x.name
        )


@pytest.fixture
def all_request_types(request_type):
    return request_type


@pytest.fixture
def pool_request(request_type):
    return request_type


# disable auto generated freshness batches to have more control
# over PP times in replicas
# Note. there might be also one more kind of generated batches:
#   - batch with Audit ledger txn after a view change
@pytest.fixture
def turn_off_freshness_state_update(txnPoolNodeSet, monkeypatch):
    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            monkeypatch.setattr(replica.config, 'UPDATE_STATE_FRESHNESS', False)


@pytest.fixture
def max_last_accepted_pre_prepare_time(looper, txnPoolNodeSet):
    ensure_all_nodes_have_same_data(looper, txnPoolNodeSet)
    pp_times = []
    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            if replica.last_accepted_pre_prepare_time:
                pp_times.append(replica.last_accepted_pre_prepare_time)
    return max(pp_times)


@pytest.fixture(params=ValidationType, ids=lambda x: x.name)
def validation_type(request):
    return request.param


@pytest.fixture
def validation_error(validation_type):
    return {
        ValidationType.FuncApi: InvalidClientTaaAcceptanceError,
        ValidationType.TxnApi: RequestRejectedException,
    }[validation_type]


@pytest.fixture
def validate_taa_acceptance(
    looper,
    sdk_wallet_new_steward,
    validate_taa_acceptance_func_api,
    validate_taa_acceptance_txn_api,
    validation_type,
):
    def wrapped(req_dict):
        req_dict = sign_request_dict(looper, sdk_wallet_new_steward, req_dict)
        {
            ValidationType.FuncApi: validate_taa_acceptance_func_api,
            ValidationType.TxnApi: validate_taa_acceptance_txn_api
        }[validation_type](req_dict)

    return wrapped


@pytest.fixture
def taa_accepted(request, random_taa, get_txn_author_agreement):
    marker = request.node.get_marker('taa_accepted')
    if marker:
        return marker.args[0]
    else:
        current_taa = get_txn_author_agreement()
        return (
            (None, None) if current_taa is None else
            (current_taa.text, current_taa.version)
        )


@pytest.fixture
def taa_acceptance_mechanism(request, aml_request_kwargs):
    marker = request.node.get_marker('taa_acceptance_mechanism')
    if marker:
        return marker.args[0]
    else:
        return list(aml_request_kwargs['operation'][AML].items())[0][0]


@pytest.fixture
def taa_acceptance_time(request, get_txn_author_agreement):
    marker = request.node.get_marker('taa_acceptance_time')
    if marker:
        return marker.args[0]
    else:
        current_taa = get_txn_author_agreement()
        return (
            get_utc_epoch() if current_taa is None else
            current_taa.txn_time
        )


@pytest.fixture
def taa_acceptance(
    request, taa_accepted, taa_acceptance_mechanism, taa_acceptance_time
):
    return {
        f.TAA_TEXT.nm: taa_accepted[0],
        f.TAA_VERSION.nm: taa_accepted[1],
        f.TAA_ACCEPTANCE_MECHANISM.nm: taa_acceptance_mechanism,
        f.TAA_ACCEPTANCE_TIME.nm: taa_acceptance_time,
    }


@pytest.fixture
def domain_request_json(looper, sdk_wallet_new_steward):
    return build_nym_request(looper, sdk_wallet_new_steward)


@pytest.fixture
def pool_request_json(looper, tconf, tdir, sdk_wallet_new_steward):
    return build_node_request(looper, tconf, tdir, sdk_wallet_new_steward)


@pytest.fixture
def request_json(request_type, domain_request_json, pool_request_json):
    return {
        RequestType.Domain: domain_request_json,
        RequestType.Pool: pool_request_json
    }[request_type]


@pytest.fixture
def add_taa_acceptance(
    looper,
    request_json,
    taa_accepted,
    taa_acceptance_mechanism,
    taa_acceptance_time
):
    def wrapped(
        req_json=None,
        taa_text=None,
        taa_version=None,
        taa_a_mech=None,
        taa_a_time=None
    ):
        return _add_taa_acceptance(
            looper,
            request_json if req_json is None else req_json,
            taa_accepted[0] if taa_text is None else taa_text,
            taa_accepted[1] if taa_version is None else taa_version,
            taa_acceptance_mechanism if taa_a_mech is None else taa_a_mech,
            taa_acceptance_time if taa_a_time is None else taa_a_time
        )

    return wrapped


@pytest.fixture
def request_dict(request, looper, request_json, add_taa_acceptance):
    if not request.node.get_marker('taa_acceptance_missed'):
        request_json = add_taa_acceptance()
    return dict(**json.loads(request_json))
