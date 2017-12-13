import pytest

from plenum.test.malicious_behaviors_node import delaysCommitProcessing
from plenum.test.test_node import getNonPrimaryReplicas
from stp_core.common.log import getlogger
from plenum.test.helper import sendRandomRequest, \
    waitForSufficientRepliesForRequests


nodeCount = 4
logger = getlogger()


# noinspection PyIncorrectDocstring
def test_working_has_no_warn_log_msg(looper, nodeSet,
                                     wallet1, client1, patch_monitors):
    monitor = nodeSet[0].monitor
    assert no_any_warn(*nodeSet)

    for i in range(monitor.WARN_NOT_PARTICIPATING_UNORDERED_NUM):
        req = sendRandomRequest(wallet1, client1)
        waitForSufficientRepliesForRequests(looper, client1, requests=[req])
        looper.runFor(monitor.WARN_NOT_PARTICIPATING_MIN_DIFF_SEC)

    assert no_any_warn(*nodeSet)


# noinspection PyIncorrectDocstring
def test_slow_node_has_warn_unordered_log_msg(looper,
                                              nodeSet,
                                              wallet1,
                                              client1,
                                              patch_monitors):
    npr = getNonPrimaryReplicas(nodeSet, 0)[0]
    slow_node = npr.node

    monitor = nodeSet[0].monitor
    delay = monitor.WARN_NOT_PARTICIPATING_MIN_DIFF_SEC * \
        monitor.WARN_NOT_PARTICIPATING_UNORDERED_NUM + 10
    delaysCommitProcessing(slow_node, delay=delay)

    assert no_any_warn(*nodeSet), \
        'all nodes do not have warnings before test'

    for i in range(monitor.WARN_NOT_PARTICIPATING_UNORDERED_NUM):
        req = sendRandomRequest(wallet1, client1)
        waitForSufficientRepliesForRequests(looper, client1, requests=[req])
        looper.runFor(monitor.WARN_NOT_PARTICIPATING_MIN_DIFF_SEC)

    others = [node for node in nodeSet if node.name != slow_node.name]
    assert no_any_warn(*others), \
        'others do not have warning after test'
    assert has_some_warn(slow_node), \
        'slow node has the warning'

    ordered_requests_keys_len_before = len(monitor.ordered_requests_keys)
    # wait at least windows time
    looper.runFor(monitor.WARN_NOT_PARTICIPATING_WINDOW_MINS * 60)
    req = sendRandomRequest(wallet1, client1)
    waitForSufficientRepliesForRequests(looper, client1, requests=[req])
    assert no_any_warn(*others), 'others do not have warning'
    assert no_last_warn(slow_node), \
        'the last call of warn_has_lot_unordered_requests returned False ' \
        'so slow node has no the warning for now'
    assert len(monitor.ordered_requests_keys) < ordered_requests_keys_len_before, \
        "ordered_requests_keys was cleaned up"


def no_any_warn(*nodes):
    for node in nodes:
        calls = node.monitor.spylog.getAll(node.monitor.warn_has_lot_unordered_requests)
        if any(call.result for call in calls):
            return False
    return True


def has_some_warn(*nodes):
    for node in nodes:
        calls = node.monitor.spylog.getAll(node.monitor.warn_has_lot_unordered_requests)
        if not any(call.result for call in calls):
            return False
    return True


def no_last_warn(*nodes):
    for node in nodes:
        call = node.monitor.spylog.getLast(node.monitor.warn_has_lot_unordered_requests)
        if call.result:
            return False
    return True


@pytest.fixture(scope="function")
def patch_monitors(nodeSet):
    backup = {}
    req_num = 3
    diff_sec = 1
    window_mins = 0.25
    for node in nodeSet:
        backup[node.name] = (
            node.monitor.WARN_NOT_PARTICIPATING_UNORDERED_NUM,
            node.monitor.WARN_NOT_PARTICIPATING_MIN_DIFF_SEC,
            node.monitor.WARN_NOT_PARTICIPATING_WINDOW_MINS,
        )
        node.monitor.WARN_NOT_PARTICIPATING_UNORDERED_NUM = req_num
        node.monitor.WARN_NOT_PARTICIPATING_MIN_DIFF_SEC = diff_sec
        node.monitor.WARN_NOT_PARTICIPATING_WINDOW_MINS = window_mins
    yield req_num, diff_sec, window_mins
    for node in nodeSet:
        node.monitor.WARN_NOT_PARTICIPATING_UNORDERED_NUM = backup[node.name][0]
        node.monitor.WARN_NOT_PARTICIPATING_MIN_DIFF_SEC = backup[node.name][1]
        node.monitor.WARN_NOT_PARTICIPATING_WINDOW_MINS = backup[node.name][2]
