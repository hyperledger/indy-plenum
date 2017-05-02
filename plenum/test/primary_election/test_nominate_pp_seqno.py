def test_nominate_after_timeout():
    """
    One node lags behind others and has not seen all 3PC messages, hence not
    ordered as many requests as others so it sends Nomination only after the
    timeout for the first one it saw nomination from. Others have ordered same
    number of requests so they all send Nomination as soon as they get one
    """
    #TODO


def test_not_nominate_when_received_higher_seq():
    pass
