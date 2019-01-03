import functools
from hashlib import sha256

import pytest

from plenum.common.request import Request
from plenum.server.propagator import Requests


@pytest.fixture(scope="function")
def requests():
    def _getDigest(req: Request):
        return sha256(req.identifier.encode())

    requests = Requests()

    req = Request("1")
    req.getDigest = functools.partial(_getDigest, req)
    requests.add(req)

    assert len(requests) == 1
    return requests, req.key


def test_unordered_by_replicas_num(requests):
    replicas_num = 3

    _requests, req_key = requests

    req_state = _requests[req_key]
    assert req_state.unordered_by_replicas_num == 0

    _requests.mark_as_forwarded(req_state.request, replicas_num)
    assert req_state.unordered_by_replicas_num == replicas_num

    for i in range(1, replicas_num + 1):
        _requests.ordered_by_replica(req_key)
        assert req_state.unordered_by_replicas_num == replicas_num - i
