def test_chaotic():
    """
    Once nodes start, introduce arbitrary delays so view change happens and
    elections happen, but now again after some requests primary loses
    connection and view change happens and this time election messages are
    received slowly but again elections complete and some more requests succeed.
    Now add a single node and see that it processes requests. Now add more nodes
    such that f changes and it still processes requests.
    This test would remain a WIP till some time, break it into multiple tests
    """
    # TODO
