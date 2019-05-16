import pytest

from plenum.common.types import f
from plenum.config import (
    TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA_TIME,
    TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_PP_TIME
)
from plenum.common.constants import AML
from plenum.common.util import get_utc_epoch

from plenum.test.input_validation.helper import (
    gen_nym_operation, gen_node_operation
)
from ..helper import calc_taa_digest
from .helper import gen_signed_request


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
def validate_taa_acceptance(node_validator):
    def wrapped(req, pp_time=None):
        return node_validator.validateTaaAcceptance(
            req, get_utc_epoch() if pp_time is None else pp_time
        )
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
def domain_req(
    request, looper, sdk_wallet_new_steward, domain_operation, taa_acceptance
):
    if request.node.get_marker('taa_acceptance_missed'):
        taa_acceptance = None
    return gen_signed_request(
        looper, sdk_wallet_new_steward, domain_operation, taa_acceptance
    )


@pytest.fixture
def req(request, looper, sdk_wallet_new_steward, operation, taa_acceptance):
    if request.node.get_marker('taa_acceptance_missed'):
        taa_acceptance = None
    return gen_signed_request(
        looper, sdk_wallet_new_steward, operation, taa_acceptance
    )
