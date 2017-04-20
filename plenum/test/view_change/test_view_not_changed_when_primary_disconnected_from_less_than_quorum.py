def test_view_not_change_when_primary_disconnected_from_less_than_quorum():
    """
    Less than quorum nodes lose connection with primary, this should not
    trigger view change as the protocol can move ahead
    """
    # TODO