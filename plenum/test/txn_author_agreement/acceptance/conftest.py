import json
from enum import Enum
import pytest

from plenum.common.types import f
from plenum.config import (
    TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA_TIME,
    TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME
)
from plenum.common.constants import AML
from plenum.common.util import get_utc_epoch
from plenum.common.request import SafeRequest
from plenum.common.exceptions import (
    InvalidClientTaaAcceptanceError, RequestRejectedException
)

from plenum.test.helper import sdk_send_and_check
from plenum.test.input_validation.helper import (
    gen_nym_operation, gen_node_operation
)
from ..helper import calc_taa_digest
from .helper import (
    gen_signed_request_obj, gen_signed_request_json
)


class ValidationType(Enum):
    FuncApi = 0
    TxnApi = 1


@pytest.fixture(scope='module')
def activate_taa(sdk_wallet_new_steward, activate_taa):
    return activate_taa


@pytest.fixture(scope='module')
def latest_taa(activate_taa):
    return activate_taa


@pytest.fixture(scope="module")
def node_validator(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture(scope='module')
def validate_taa_acceptance_func_api(node_validator):
    def wrapped(signed_req_obj, pp_time=None):
        node_validator.validateTaaAcceptance(
            signed_req_obj, get_utc_epoch() if pp_time is None else pp_time
        )
    return wrapped


@pytest.fixture(scope='module')
def validate_taa_acceptance_txn_api(looper, txnPoolNodeSet, sdk_pool_handle):
    def wrapped(monkeypatch, signed_req_json, pp_time=None):
        # TODO get_current_time patching ???
        if pp_time is not None:
            for node in txnPoolNodeSet:
                for replica in node.replicas.values():
                    monkeypatch.setattr(replica, 'get_time_for_3pc_batch', lambda: pp_time)
        sdk_send_and_check([signed_req_json], looper, txnPoolNodeSet, sdk_pool_handle)[0]
        # TODO monkeypatch.undo undos all patches, it might inappropriate for some cases
        if pp_time is not None:
            monkeypatch.undo()
    return wrapped


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
    validate_taa_acceptance_func_api,
    validate_taa_acceptance_txn_api,
    validation_type,
    monkeypatch
):
    def wrapped(*args, **kwargs):
        if validation_type == ValidationType.FuncApi:
            validate_taa_acceptance_func_api(*args, **kwargs)
        else:
            assert validation_type == ValidationType.TxnApi
            validate_taa_acceptance_txn_api(monkeypatch, *args, **kwargs)

    return wrapped


@pytest.fixture
def taa_digest(random_taa, get_txn_author_agreement):
    current_taa = get_txn_author_agreement()
    text, version = (
        random_taa if current_taa is None else
        (current_taa.text, current_taa.version)
    )
    return calc_taa_digest(text, version)


@pytest.fixture(scope="module")
def taa_acceptance_mechanism(aml_request_kwargs):
    return list(aml_request_kwargs['operation'][AML].items())[0][0]


@pytest.fixture
def taa_acceptance_time():
    return get_utc_epoch()


@pytest.fixture
def taa_acceptance(request, taa_digest, taa_acceptance_mechanism, taa_acceptance_time):
    digest_marker = request.node.get_marker('taa_acceptance_digest')
    mech_marker = request.node.get_marker('taa_acceptance_mechanism')
    time_marker = request.node.get_marker('taa_acceptance_time')
    return {
        f.TAA_ACCEPTANCE_DIGEST.nm:
            digest_marker.args[0] if digest_marker else taa_digest,
        f.TAA_ACCEPTANCE_MECHANISM.nm:
            mech_marker.args[0] if mech_marker else taa_acceptance_mechanism,
        f.TAA_ACCEPTANCE_TIME.nm:
            time_marker.args[0] if time_marker else taa_acceptance_time,
    }


@pytest.fixture
def domain_operation():
    return gen_nym_operation()


@pytest.fixture
def pool_operation():
    return gen_node_operation()


@pytest.fixture(
    params=['pool_op', 'domain_op']
)
def operation(request, domain_operation, pool_operation):
    return {
        'pool_op': pool_operation,
        'domain_op': domain_operation
    }[request.param]


@pytest.fixture
def gen_signed_request(looper, validation_type):
    generator = {
        ValidationType.FuncApi: gen_signed_request_obj,
        ValidationType.TxnApi: gen_signed_request_json,
    }

    def wrapped(sdk_wallet, operation, taa_acceptance):
        return generator[validation_type](
            looper, sdk_wallet, operation, taa_acceptance=taa_acceptance
        )

    return wrapped


@pytest.fixture
def signed_domain_req(
    request, gen_signed_request, looper,
    sdk_wallet_new_steward, domain_operation, taa_acceptance
):
    if request.node.get_marker('taa_acceptance_missed'):
        taa_acceptance = None
    return gen_signed_request(
        sdk_wallet_new_steward, domain_operation, taa_acceptance
    )


@pytest.fixture
def signed_req(
    request, gen_signed_request, looper,
    sdk_wallet_new_steward, operation, taa_acceptance
):
    if request.node.get_marker('taa_acceptance_missed'):
        taa_acceptance = None
    return gen_signed_request(
        sdk_wallet_new_steward, operation, taa_acceptance
    )


@pytest.fixture
def req_obj(signed_req, validation_type):
    return {
        ValidationType.FuncApi: lambda: signed_req,
        ValidationType.TxnApi: lambda: SafeRequest(**json.loads(signed_req)),
    }[validation_type]()
