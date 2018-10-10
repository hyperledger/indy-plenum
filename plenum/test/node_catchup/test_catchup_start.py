import pytest

from plenum.common.startable import Mode

catchup_started = False


def _mocked_do_start_catchup(just_started):
    global catchup_started
    catchup_started = True


@pytest.fixture(scope='module')
def _patched_node(txnPoolNodeSet):
    node = txnPoolNodeSet[0]
    node._do_start_catchup = _mocked_do_start_catchup
    return node


@pytest.fixture(scope='function')
def node(_patched_node):
    global catchup_started
    catchup_started = False
    return _patched_node


def test_catchup_can_be_started_with_just_started_flag_when_mode_not_set(node):
    node.mode = None
    node.start_catchup(just_started=True)
    assert catchup_started


def test_catchup_cannot_be_started_without_just_started_flag_when_mode_not_set(
        node):
    node.mode = None
    node.start_catchup()
    assert not catchup_started


def test_catchup_cannot_be_started_in_starting_mode(node):
    node.mode = Mode.starting
    node.start_catchup()
    assert not catchup_started


def test_catchup_cannot_be_started_in_discovering_mode(node):
    node.mode = Mode.discovering
    node.start_catchup()
    assert not catchup_started


def test_catchup_cannot_be_started_in_discovered_mode(node):
    node.mode = Mode.discovered
    node.start_catchup()
    assert not catchup_started


def test_catchup_cannot_be_started_in_syncing_mode(node):
    node.mode = Mode.syncing
    node.start_catchup()
    assert not catchup_started


def test_catchup_can_be_started_in_synced_mode(node):
    node.mode = Mode.synced
    node.start_catchup()
    assert catchup_started


def test_catchup_can_be_started_in_participating_mode(node):
    node.mode = Mode.participating
    node.start_catchup()
    assert catchup_started
