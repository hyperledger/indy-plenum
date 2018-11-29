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
    node.mode = Mode.participating
    return _patched_node


@pytest.mark.parametrize('mode, catchup_must_start', [
    (Mode.starting, False),
    (Mode.discovering, False),
    (Mode.discovered, False),
    (Mode.syncing, False),
    (Mode.synced, True),
    (Mode.participating, True),
])
def test_catchup_ability_in_given_mode(node, mode, catchup_must_start):
    node.mode = mode
    node.start_catchup()
    assert catchup_started is catchup_must_start


def test_catchup_can_be_started_with_just_started_flag_when_mode_not_set(node):
    node.mode = None
    node.start_catchup(just_started=True)
    assert catchup_started


def test_catchup_cannot_be_started_without_just_started_flag_when_mode_not_set(
        node):
    node.mode = None
    node.start_catchup()
    assert not catchup_started
