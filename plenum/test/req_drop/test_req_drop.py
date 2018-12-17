import functools
from _sha256 import sha256

import pytest

from plenum.common.request import Request
from plenum.server.node import Node
from plenum.server.propagator import Requests
from plenum.test.testing_utils import FakeSomething


req_identifiers = ["1", "2", "3"]


@pytest.fixture(scope="function", params=['propagates', 'ordering'])
def phase(request):
    return request.param


@pytest.fixture(scope="function")
def requests():
    def _getDigest(req: Request):
        return sha256(req.identifier.encode())

    requests = Requests()
    for id in req_identifiers:
        req = Request(id)
        req.getDigest = functools.partial(_getDigest, req)
        requests.add(req)

    assert len(requests) == 3
    return requests


@pytest.fixture(scope="function")
def node(requests):
    fake_monitor = FakeSomething(requestTracker=FakeSomething(
        force_req_drop=lambda *args, **kwargs: True))
    return FakeSomething(requests=requests,
                         propagates_phase_req_timeout=1,
                         ordering_phase_req_timeout=1,
                         propagates_phase_req_timeouts=0,
                         ordering_phase_req_timeouts=0,
                         monitor=fake_monitor,
                         _clean_req_from_verified=lambda *args, **kwargs: True,
                         doneProcessingReq=lambda *args, **kwargs: True)


def __prepare_req_for_drop(requests, req_identifier, phase):
    req_key_to_drop = None
    for req_key in requests:
        req_state = requests[req_key]
        if req_state.request.identifier == req_identifier:
            if phase == "propagates":
                req_state.added_ts = 0
            else:
                req_state.added_ts = None
                req_state.finalised_ts = 0
            req_key_to_drop = req_key
            break
    assert req_key_to_drop != None
    return req_key_to_drop


def test_no_drops(node):
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 3
    assert node.propagates_phase_req_timeouts == 0
    assert node.ordering_phase_req_timeouts == 0


def test_drop_first_req(node, phase):
    req_identifier = req_identifiers[0]
    req_key_to_drop = __prepare_req_for_drop(node.requests, req_identifier, phase)
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 2
    assert req_key_to_drop not in node.requests
    if phase == "propagates":
        assert node.propagates_phase_req_timeouts == 1
        assert node.ordering_phase_req_timeouts == 0
    elif phase == "ordering":
        assert node.propagates_phase_req_timeouts == 0
        assert node.ordering_phase_req_timeouts == 1


def test_drop_middle_req(node, phase):
    req_identifier = req_identifiers[1]
    req_key_to_drop = __prepare_req_for_drop(node.requests, req_identifier, phase)
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 2
    assert req_key_to_drop not in node.requests
    if phase == "propagates":
        assert node.propagates_phase_req_timeouts == 1
        assert node.ordering_phase_req_timeouts == 0
    elif phase == "ordering":
        assert node.propagates_phase_req_timeouts == 0
        assert node.ordering_phase_req_timeouts == 1


def test_drop_last_req(node, phase):
    req_identifier = req_identifiers[2]
    req_key_to_drop = __prepare_req_for_drop(node.requests, req_identifier, phase)
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 2
    assert req_key_to_drop not in node.requests
    if phase == "propagates":
        assert node.propagates_phase_req_timeouts == 1
        assert node.ordering_phase_req_timeouts == 0
    elif phase == "ordering":
        assert node.propagates_phase_req_timeouts == 0
        assert node.ordering_phase_req_timeouts == 1


def test_drop_two_first_reqs(node):
    rek_keys_to_drop = []
    rek_keys_to_drop.append(__prepare_req_for_drop(node.requests, "1", "propagates"))
    rek_keys_to_drop.append(__prepare_req_for_drop(node.requests, "2", "ordering"))
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 1
    assert node.propagates_phase_req_timeouts == 1
    assert node.ordering_phase_req_timeouts == 1


def test_drop_two_last_reqs(node):
    rek_keys_to_drop = []
    rek_keys_to_drop.append(__prepare_req_for_drop(node.requests, "2", "propagates"))
    rek_keys_to_drop.append(__prepare_req_for_drop(node.requests, "3", "ordering"))
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 1
    assert node.propagates_phase_req_timeouts == 1
    assert node.ordering_phase_req_timeouts == 1


def test_drop_all_reqs(node, phase):
    for req_identifier in req_identifiers:
        __prepare_req_for_drop(node.requests, req_identifier, phase)
    Node.check_outdated_reqs(node)
    assert len(node.requests) == 0
    if phase == "propagates":
        assert node.propagates_phase_req_timeouts == 3
        assert node.ordering_phase_req_timeouts == 0
    elif phase == "ordering":
        assert node.propagates_phase_req_timeouts == 0
        assert node.ordering_phase_req_timeouts == 3
