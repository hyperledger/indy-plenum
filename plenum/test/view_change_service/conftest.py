from functools import partial

import pytest

from plenum.server.consensus.ordering_service_msg_validator import OrderingServiceMsgValidator
from plenum.test.view_change_service.helper import trigger_view_change_on_node


@pytest.fixture(scope="module")
def tconf(tconf):
    old_new_view_timeout = tconf.NEW_VIEW_TIMEOUT
    old_batch_size = tconf.Max3PCBatchSize
    tconf.NEW_VIEW_TIMEOUT = 5
    tconf.Max3PCBatchSize = 1
    yield tconf
    tconf.Max3PCBatchSize = old_batch_size
    tconf.NEW_VIEW_TIMEOUT = old_new_view_timeout


@pytest.fixture(scope="module")
def txnPoolNodeSet(txnPoolNodeSet):
    for node in txnPoolNodeSet:
        for replica in node.replicas.values():
            replica._ordering_service._validator = OrderingServiceMsgValidator(replica._consensus_data)
        node._view_changer.start_view_change = partial(trigger_view_change_on_node, node)
    yield txnPoolNodeSet
