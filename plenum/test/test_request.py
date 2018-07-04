from plenum.common.request import Request


def test_request_all_identifiers_returns_empty_list_for_request_without_signatures():
    req = Request()

    assert req.all_identifiers == []