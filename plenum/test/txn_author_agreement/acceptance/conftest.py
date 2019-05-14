import pytest
import hashlib

from plenum.common.constants import (
    CURRENT_PROTOCOL_VERSION, TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE,
    TXN_TYPE, NODE, TARGET_NYM, DATA
)
from plenum.common.types import OPERATION, f
from plenum.common.util import get_utc_epoch
from plenum.common.request import SafeRequest

from plenum.test.conftest import getValueFromModule
from plenum.test.input_validation.conftest import operation as nym_operation, taa_acceptance
from plenum.test.input_validation.constants import TEST_TARGET_NYM

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
def validate(node_validator):
    def wrapped(req):
        return node_validator.doDynamicValidation(req)
    return wrapped


# TODO partly copies plenum/test/input_validation/test_client_safe_request.py
@pytest.fixture
def kwargs_base():
    return {
        'identifier': '1' * 16,
        'reqId': 1,
        'protocolVersion': CURRENT_PROTOCOL_VERSION,
    }


@pytest.fixture(scope='module')
def taa_digest():
    return hashlib.sha256(b'some-taa').hexdigest()


@pytest.fixture
def taa_acceptance_mechanism():
    return 'some-mechanism'


@pytest.fixture
def taa_acceptance_time():
    return get_utc_epoch()


@pytest.fixture
def taa_acceptance(taa_digest, taa_acceptance_mechanism, taa_acceptance_time):
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


@pytest.fixture    # noqa: F811
def kwargs_no_operation(kwargs_base, taa_acceptance, request):
    marker = request.node.get_marker('taa_acceptance_missed')
    if not marker:
        kwargs_base[TXN_PAYLOAD_METADATA_TAA_ACCEPTANCE] = taa_acceptance
    return kwargs_base


@pytest.fixture     # noqa: F811
def domain_operation(nym_operation):
    return nym_operation


@pytest.fixture
def domain_req(kwargs_no_operation, domain_operation):
    kwargs_no_operation[OPERATION] = domain_operation
    return SafeRequest(**kwargs_no_operation)


@pytest.fixture(
    params=['pool_op', 'domain_op']
)
def operation(request, domain_operation):
    if request.param == 'pool_op':
        return {
            TXN_TYPE: NODE,
            TARGET_NYM: TEST_TARGET_NYM,
            DATA: {}
        }
    else:
        assert request.param == 'domain_op'
        return domain_operation


@pytest.fixture
def req(kwargs_no_operation, operation):
    kwargs_no_operation[OPERATION] = operation
    return SafeRequest(**kwargs_no_operation)
