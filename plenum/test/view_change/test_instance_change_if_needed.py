import pytest


@pytest.fixture(scope="module")
def tconf(tconf):
    OLD_INSTANCE_CHANGE_TIMEOUT = tconf.INSTANCE_CHANGE_TIMEOUT
    tconf.INSTANCE_CHANGE_TIMEOUT = 0.3

    yield tconf

    tconf.INSTANCE_CHANGE_TIMEOUT = OLD_INSTANCE_CHANGE_TIMEOUT


def test_instance_change_from_known(looper, fake_view_changer, tconf):
    # Primary was disconnected
    fake_view_changer.node.lost_primary_at = True
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
    fake_view_changer.node.lost_primary_at = False
    fake_view_changer.node.nodestack.conns.add('Alpha')

    for _ in range(times):
        looper.runFor(tconf.INSTANCE_CHANGE_TIMEOUT)
        fake_view_changer._serviceActions()
        # Instance change counter dropped because primary
        # reconnected and we do not send INSTANCE_CHANGE anymore
        assert fake_view_changer.instance_change_rounds == 0
