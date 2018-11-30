from logging import getLogger

from plenum.test.testing_utils import FakeSomething

from plenum.test.primary_selection.test_primary_selector import FakeNode

logger = getLogger()

ledger_id = 1


def test_free_finalized_sended_catchuped_requests(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].prePrepares[(0, 1)] = FakeSomething(ledgerId=0, reqIdr=[key])
    node.replicas[0].requestQueues[ledger_id].add(key)

    # "Catching up" request
    node.seqNoDB[key] = (True, True)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request freed and deleted from requestQueues
    assert node.requests.get(key).executed
    assert not key in node.replicas[0].requestQueues[ledger_id]


def test_free_finalized_non_sended_catchuped_requests(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].requestQueues[ledger_id].add(key)

    # "Catching up" request
    node.seqNoDB[key] = (True, True)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request freed and deleted from requestQueues
    assert node.requests.get(key).executed
    assert not key in node.replicas[0].requestQueues[ledger_id]


def test_free_non_finalized_catchuped_requests(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=False)
    key = req.key
    node.requests.add(req)

    # "Catching up" request
    node.seqNoDB[key] = (True, True)
    node.ledgerManager.last_caught_up_3PC = (0, 1)
    node.allLedgersCaughtUp()

    # Request freed and deleted from requestQueues
    assert key not in node.requests


def test_dont_free_finalized_sended_requests_if_more_caught_up_3pc(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].prePrepares[(0, 2)] = FakeSomething(ledgerId=0, reqIdr=[key])
    node.replicas[0].requestQueues[ledger_id].add(key)

    # Request freed and deleted from requestQueues
    assert not node.requests.get(key).executed
    assert key in node.replicas[0].requestQueues[ledger_id]


def test_free_finalized_non_sended_if_more_caught_up_3pc(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=True)
    key = req.key
    node.requests.add(req)
    node.requests.mark_as_forwarded(req, 3)
    node.replicas[0].requestQueues[ledger_id].add(key)

    # Request freed and deleted from requestQueues
    assert not node.requests.get(key).executed
    assert key in node.replicas[0].requestQueues[ledger_id]


def test_free_non_finalized_which_isnt_ordered(
        tdir, tconf):
    node = FakeNode(tdir, config=tconf)

    # "Processing request"
    req = FakeSomething(key='request_1', forwarded=False)
    key = req.key
    node.requests.add(req)

    # Request freed and deleted from requestQueues
    assert key in node.requests
