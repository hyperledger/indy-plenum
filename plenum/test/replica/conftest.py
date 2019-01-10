import types

import pytest

from plenum.common.startable import Mode
from plenum.common.util import get_utc_epoch
from plenum.server.node import Node
from plenum.server.quorums import Quorums
from plenum.server.replica import Replica
from plenum.test.conftest import getValueFromModule
from plenum.test.testing_utils import FakeSomething


class ReplicaFakeNode(FakeSomething):

    def __init__(self, viewNo, quorums):
        node_stack = FakeSomething(
            name="fake stack",
            connecteds={"Alpha", "Beta", "Gamma", "Delta"}
        )
        super().__init__(
            name="fake node",
            ledger_ids=[0],
            viewNo=viewNo,
            quorums=quorums,
            nodestack=node_stack,
            utc_epoch=lambda *args: get_utc_epoch(),
            mode=Mode.participating,
            view_change_in_progress=False
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

@pytest.fixture(scope='function', params=[1])
def inst_id(request):
    return request.param

@pytest.fixture(scope='function')
def replica(tconf, viewNo, inst_id, request):
    node = ReplicaFakeNode(viewNo=viewNo,
                           quorums=Quorums(getValueFromModule(request, 'nodeCount', default=4)))
    bls_bft_replica = FakeSomething(
        gc=lambda *args: None,
        update_pre_prepare=lambda params, l_id: params
    )
    replica = Replica(
        node, instId=inst_id, isMaster=inst_id == 0,
        config=tconf, bls_bft_replica=bls_bft_replica
    )
    ReplicaFakeNode.master_last_ordered_3PC = replica.last_ordered_3pc
    return replica