import pytest

from common.exceptions import LogicError
from plenum.common.constants import TXN_TYPE
from plenum.common.request import Request


def test_on_view_change_complete_fails(test_node):
    with pytest.raises(LogicError) as excinfo:
       test_node.on_view_change_complete()
    assert "Not all replicas have primaries" in str(excinfo.value)


def test_ledger_id_for_request_fails(test_node):
    for r in (Request(operation={}), Request(operation={TXN_TYPE: None})):
        with pytest.raises(ValueError) as excinfo:
           test_node.ledger_id_for_request(r)
        assert "TXN_TYPE is not defined for request" in str(excinfo.value)
