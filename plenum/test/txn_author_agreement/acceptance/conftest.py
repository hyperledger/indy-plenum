import pytest

from plenum.common.types import f
from plenum.config import (
    TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA,
    TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_NOW
)
from plenum.common.util import get_utc_epoch

from plenum.test.conftest import getValueFromModule
from plenum.test.replica.conftest import primary_replica as _primary_replica
from plenum.test.input_validation.helper import (
    gen_nym_operation, gen_node_operation
)
from ..helper import calc_taa_digest
from .helper import gen_signed_request


TAA_ACCEPTANCE_TS_TOO_OLD = 0
TAA_LATEST_TS = TAA_ACCEPTANCE_TS_TOO_OLD + TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_BEFORE_TAA + 1
TS_NOW = TAA_LATEST_TS + 1
TAA_ACCEPTANCE_TS_TOO_RECENT = TS_NOW + TXN_AUTHOR_AGREEMENT_ACCEPANCE_TIME_AFTER_NOW + 1


@pytest.fixture(scope="module")
def taa_a_ts_too_old():
    return TAA_ACCEPTANCE_TS_TOO_OLD


@pytest.fixture(scope="module")
def taa_a_ts_too_recent():
    return TAA_ACCEPTANCE_TS_TOO_RECENT


@pytest.fixture(scope="module")
def ts_now():
    # TODO use tconf
    return TS_NOW


@pytest.fixture(scope="module")
def taa_latest_ts(ts_now):
    return TAA_LATEST_TS


@pytest.fixture
def primary_replica(_primary_replica):
    _primary_replica.last_accepted_pre_prepare_time = None
    _primary_replica.get_time_for_3pc_batch.value = TS_NOW
    #_primary_replica.threePhaseRouter.add((PrePrepare, lambda *x, **y: None))
    return _primary_replica


@pytest.fixture(scope="module")
def node_validator(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    #node.now = TAA_LATEST_TS
    return node


@pytest.fixture(scope="module")
def validate_taa_acceptance(node_validator):
    def wrapped(req):
        return node_validator.validateTaaAcceptance(req)
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
