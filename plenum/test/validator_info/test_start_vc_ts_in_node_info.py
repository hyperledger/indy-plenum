import datetime
from contextlib import ExitStack
from dateutil import parser

import pytest

from plenum.test.helper import create_new_test_node


@pytest.fixture(scope="module")
def create_node_and_not_start(testNodeClass,
                              node_config_helper_class,
                              tconf,
                              tdir,
                              allPluginsPath,
                              looper,
                              tdirWithPoolTxns,
                              tdirWithDomainTxns,
                              tdirWithNodeKeepInited):
    with ExitStack() as exitStack:
        node = exitStack.enter_context(create_new_test_node(testNodeClass,
                                node_config_helper_class,
                                "Alpha",
                                tconf,
                                tdir,
                                allPluginsPath))
        yield node
        node.stop()


def test_start_view_change_ts_set(looper, create_node_and_not_start):
    node = create_node_and_not_start
    node.start(looper)
    node.on_view_change_start()
    node_info = node._info_tool.info
    assert "Last_view_change_started_at" in node_info["Node_info"]["View_change_status"]
    assert datetime.datetime.utcfromtimestamp(
        node.start_view_change_ts) == parser.parse(
        node_info["Node_info"]["View_change_status"]["Last_view_change_started_at"])
