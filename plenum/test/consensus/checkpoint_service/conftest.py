import pytest

from plenum.common.startable import Mode
from plenum.common.stashing_router import StashingRouter
from plenum.server.consensus.checkpoint_service import CheckpointService
from plenum.test.helper import MockNetwork
from plenum.test.testing_utils import FakeSomething


@pytest.fixture()
def stasher(tconf):
    return StashingRouter(tconf.REPLICA_STASH_LIMIT)


@pytest.fixture()
def checkpoint_service(consensus_data, internal_bus,
                       bls_bft_replica, stasher, db_manager):
    checkpoint_service = CheckpointService(data=consensus_data("CheckpointService"),
                                           bus=internal_bus,
                                           network=MockNetwork(),
                                           stasher=stasher,
                                           db_manager=db_manager,
                                           old_stasher=FakeSomething(unstash_watermarks=lambda: None))
    checkpoint_service._data.node_mode = Mode.participating
    return checkpoint_service
