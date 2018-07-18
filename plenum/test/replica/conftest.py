import pytest

from plenum.common.util import get_utc_epoch
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.test.conftest import getValueFromModule
from plenum.test.testing_utils import FakeSomething


@pytest.fixture(scope='function', params=[0, 10])
def replica(tconf, request):
    node_stack = FakeSomething(
        name="fake stack",
        connecteds={"Alpha", "Beta", "Gamma", "Delta"}
    )
    node = FakeSomething(
        name="fake node",
        ledger_ids=[0],
        viewNo=request.param,
        quorums=Quorums(getValueFromModule(request, 'nodeCount', default=4)),
        nodestack=node_stack,
        utc_epoch=lambda *args: get_utc_epoch()
    )
    bls_bft_replica = FakeSomething(
        gc=lambda *args: None,
    )
    replica = Replica(
        node, instId=0, isMaster=False,
        config=tconf, bls_bft_replica=bls_bft_replica
    )
    return replica