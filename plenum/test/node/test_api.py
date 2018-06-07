import pytest

from common.exceptions import LogicError
from plenum.test.test_node import TestNode
from plenum.common.constants import TXN_TYPE
from plenum.common.config_helper import PNodeConfigHelper
from plenum.common.request import Request


@pytest.fixture(scope='function')
def test_node(
        tdirWithPoolTxns,
        tdirWithDomainTxns,
        poolTxnNodeNames,
        tdirWithNodeKeepInited,
        tdir,
        tconf,
        allPluginsPath):

    node_name = poolTxnNodeNames[0]
    config_helper = PNodeConfigHelper(node_name, tconf, chroot=tdir)
    node = TestNode(
        node_name,
        config_helper=config_helper,
        config=tconf,
        pluginPaths=allPluginsPath)
    yield node
    node.onStopping() # TODO stop won't call onStopping as we are in Stopped state


def test_on_view_change_complete_fails(test_node):
    with pytest.raises(LogicError) as excinfo:
       test_node.on_view_change_complete()
    assert "Not all replicas have primaries" in str(excinfo.value)


def test_ledger_id_for_request_fails(test_node):
    for r in (Request(operation={}), Request(operation={TXN_TYPE: None})):
        with pytest.raises(ValueError) as excinfo:
           test_node.ledger_id_for_request(r)
        assert "TXN_TYPE is not defined for request" in str(excinfo.value)
