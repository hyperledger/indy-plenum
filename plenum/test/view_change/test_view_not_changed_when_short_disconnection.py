def test_view_not_changed_when_short_disconnection():
    """
    When primary is disconnected but not long enough to trigger the timeout,
    view change should not happen
    """
    # TODO