import pytest

from plenum.common.startable import Mode
from plenum.server.consensus.checkpoint_service import CheckpointService


@pytest.fixture()
def checkpoint_service(consensus_data, internal_bus, external_bus,
                       bls_bft_replica, stasher, db_manager):
    checkpoint_service = CheckpointService(data=consensus_data("CheckpointService"),
                                           bus=internal_bus,
                                           network=external_bus,
                                           stasher=stasher,
                                           db_manager=db_manager)
    checkpoint_service._data.node_mode = Mode.participating
    return checkpoint_service
