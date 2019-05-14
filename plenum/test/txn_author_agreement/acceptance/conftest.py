import pytest

from plenum.common.types import f
from plenum.common.util import get_utc_epoch

from plenum.test.conftest import getValueFromModule
from plenum.test.input_validation.helper import (
    gen_nym_operation, gen_node_operation
)
from ..helper import calc_taa_digest
from .helper import gen_signed_request

TAA_ACCEPTANCE_TS_LOWER_INTERVAL = 120
TAA_ACCEPTANCE_TS_HIGHER_INTERVAL = 120

TAA_ACCEPTANCE_TS_TOO_OLD = 0
LATEST_TAA_TS = TAA_ACCEPTANCE_TS_TOO_OLD + TAA_ACCEPTANCE_TS_LOWER_INTERVAL + 1
TS_NOW = LATEST_TAA_TS + 1
TAA_ACCEPTANCE_TS_TOO_RECENT = TS_NOW + TAA_ACCEPTANCE_TS_HIGHER_INTERVAL + 1



@pytest.fixture(scope="module")
def txnPoolNodeSet(txnPoolNodeSet, request, set_txn_author_agreement):
    taa_disabled = getValueFromModule(request, "TAA_DISABLED", False)

    if not taa_disabled:
        set_txn_author_agreement()

    return txnPoolNodeSet


@pytest.fixture(scope="module")
def node_validator(txnPoolNodeSet):
    return txnPoolNodeSet[0]


@pytest.fixture(scope="module")
def validate_taa_acceptance(node_validator):
    def wrapped(req):
        return node_validator.doDynamicValidation(req)
    return wrapped


@pytest.fixture
def taa_digest(random_taa):
    return calc_taa_digest(*random_taa)


@pytest.fixture
def taa_acceptance_mechanism():
    return 'some-mechanism'


@pytest.fixture
def taa_acceptance_time():
    return get_utc_epoch()


@pytest.fixture
def taa_acceptance(request, taa_digest, taa_acceptance_mechanism, taa_acceptance_time):
    digest_marker = request.node.get_marker('taa_digest')
    mech_marker = request.node.get_marker('taa_acceptance_mechanism')
    time_marker = request.node.get_marker('taa_acceptance_time')
    return {
        f.TAA_ACCEPTANCE_DIGEST.nm:
            digest_marker.args[0] if digest_marker else taa_digest,
        f.TAA_ACCEPTANCE_MECHANISM.nm:
            digest_marker.args[0] if digest_marker else taa_acceptance_mechanism,
        f.TAA_ACCEPTANCE_TIME.nm:
            digest_marker.args[0] if digest_marker else taa_acceptance_time,
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
