import pytest
import time

from plenum.server.suspicion_codes import Suspicions


@pytest.fixture(scope="module")
def tconf(tconf):
    OLD_INSTANCE_CHANGE_TIMEOUT = tconf.INSTANCE_CHANGE_TIMEOUT
    tconf.INSTANCE_CHANGE_TIMEOUT = 0.3

    yield tconf

    tconf.INSTANCE_CHANGE_TIMEOUT = OLD_INSTANCE_CHANGE_TIMEOUT


@pytest.fixture(params=[0])
def fake_view_changer(fake_view_changer):
    return fake_view_changer


def test_instance_change_on_primary_disconnected(looper, fake_view_changer, tconf):
    # Primary was disconnected
    fake_view_changer.node.lost_primary_at = time.perf_counter()
    fake_view_changer.node.nodestack.conns.remove('Alpha')
    fake_view_changer.on_primary_loss()

    # Initial instance_change_rounds count is zero
    assert fake_view_changer.instance_change_rounds == 0

    times = 5
    for _ in range(times):
        looper.runFor(tconf.INSTANCE_CHANGE_TIMEOUT)
        fake_view_changer._serviceActions()

    # As long as primary would be disconnected, view_changer
    # would continue to send INSTANCE_CHANGE_MESSAGE
    assert fake_view_changer.instance_change_rounds == times

    # Primary connected
    fake_view_changer.node.lost_primary_at = None
    fake_view_changer.node.nodestack.conns.add('Alpha')

    for _ in range(times):
        looper.runFor(tconf.INSTANCE_CHANGE_TIMEOUT)
        fake_view_changer._serviceActions()
        # Instance change counter dropped because primary
        # reconnected and we do not send INSTANCE_CHANGE anymore
        assert fake_view_changer.instance_change_rounds == 0


def test_send_instance_change_if_needed_can_view_change(fake_view_changer):
    fake_view_changer.is_primary_disconnected = lambda: True
    fake_view_changer._canViewChange = lambda proposedViewNo: (True, None)

    old_instance_change_rounds = fake_view_changer.instance_change_rounds
    fake_view_changer.send_instance_change_if_needed(fake_view_changer.view_no + 1, Suspicions.PRIMARY_DISCONNECTED)

    # No INSTANCE_CHANGE was send
    assert old_instance_change_rounds == fake_view_changer.instance_change_rounds

    fake_view_changer._canViewChange = lambda proposedViewNo: (False, None)
    fake_view_changer.send_instance_change_if_needed(fake_view_changer.view_no + 1, Suspicions.PRIMARY_DISCONNECTED)

    # One INSTANCE_CHANGE was send
    assert old_instance_change_rounds + 1 == fake_view_changer.instance_change_rounds


def test_send_instance_change_if_needed_view_no(fake_view_changer):
    fake_view_changer.is_primary_disconnected = lambda: True
    fake_view_changer._canViewChange = lambda proposedViewNo: (False, None)
    old_instance_change_rounds = fake_view_changer.instance_change_rounds

    fake_view_changer.send_instance_change_if_needed(fake_view_changer.view_no - 1, Suspicions.PRIMARY_DISCONNECTED)

    # No INSTANCE_CHANGE was send
    assert old_instance_change_rounds == fake_view_changer.instance_change_rounds

    fake_view_changer.send_instance_change_if_needed(fake_view_changer.view_no + 1, Suspicions.PRIMARY_DISCONNECTED)

    # One INSTANCE_CHANGE was send
    assert old_instance_change_rounds + 1 == fake_view_changer.instance_change_rounds


def test_send_instance_change_if_needed_primary_disconnected(fake_view_changer):
    fake_view_changer._canViewChange = lambda proposedViewNo: (False, None)
    fake_view_changer.is_primary_disconnected = lambda: False
    old_instance_change_rounds = fake_view_changer.instance_change_rounds

    fake_view_changer.send_instance_change_if_needed(fake_view_changer.view_no + 1, Suspicions.PRIMARY_DISCONNECTED)

    # No INSTANCE_CHANGE was send
    assert old_instance_change_rounds == fake_view_changer.instance_change_rounds

    fake_view_changer.is_primary_disconnected = lambda: True
    fake_view_changer.send_instance_change_if_needed(fake_view_changer.view_no + 1, Suspicions.PRIMARY_DISCONNECTED)

    # No INSTANCE_CHANGE was send
    assert old_instance_change_rounds + 1 == fake_view_changer.instance_change_rounds
