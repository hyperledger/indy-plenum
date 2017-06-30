from plenum.server.replica import Replica


class FakeNode():

    def __init__(self, **kwargs):
        for name, value in kwargs.items():
            setattr(self, name, value)


def test_ordered_cleaning():


    global_view_no = 2

    node = FakeNode(
        name="fake node",
        ledger_ids=[0],
        viewNo=global_view_no,
    )
    replica = Replica(node, instId=0)
    total = []

    num_requests_per_view = 3
    for viewNo in range(global_view_no + 1):
        for seqNo in range(num_requests_per_view):
            reqId = viewNo, seqNo
            replica.addToOrdered(*reqId)
            total.append(reqId)

    # Requests with view lower then previous view
    # should not be in ordered
    assert len(replica.ordered) == len(total[num_requests_per_view:])
