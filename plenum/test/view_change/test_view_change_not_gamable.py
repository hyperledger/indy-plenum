import pytest


@pytest.mark.skip('INDY-102. Not implemented')
def test_view_change_not_gamable():
    # # TODO: A malicious node should not be able to disrupt a
    # view change by sending a message too early, this decreasing the
    # available time to get enough view change messages
    raise NotImplementedError
