import time
from typing import Any

import pytest

from plenum.common.event_bus import InternalBus
from plenum.common.messages.internal_messages import PrimariesBatchNeeded, \
    CurrentPrimaries
from plenum.server.consensus.consensus_shared_data import ConsensusSharedData
from plenum.test.consensus.conftest import mode


@pytest.fixture()
def ib(replica):
    return replica.node.internal_bus


@pytest.fixture()
def consensus_data(replica) -> ConsensusSharedData:
    return replica._consensus_data


@pytest.fixture(params=[True, False])
def vc_in_progress(request):
    return request.param


@pytest.fixture(params=[True, False])
def pb_needed(request):
    return request.param


@pytest.fixture(params=[[1,2,3], [4,5,6], [7,8,9]])
def primaries(request):
    return request.param


def send_msg(bus: InternalBus, msg: Any):
    bus.send(msg)


def test_pass_primaries_batch_needed(consensus_data, ib, pb_needed):
    send_msg(ib, PrimariesBatchNeeded(pb_needed))
    assert consensus_data.primaries_batch_needed == pb_needed


def test_pass_current_primaries(consensus_data, ib, primaries):
    send_msg(ib, CurrentPrimaries(primaries))
    assert consensus_data.primaries == primaries
