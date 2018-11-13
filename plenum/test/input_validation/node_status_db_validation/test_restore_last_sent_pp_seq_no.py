from logging import getLogger

import pytest

from common.serializers.serialization import node_status_db_serializer
from plenum.common.constants import LAST_SENT_PRE_PREPARE
from plenum.test.input_validation.helper import OutputWarningHandler
from plenum.test.primary_selection.test_primary_selector import FakeNode

logger = getLogger()
container = list()
logger.addHandler(OutputWarningHandler(container))
warning_msg_count = len(container)
initial_3pc = None


@pytest.fixture(scope="module")
def node(tconf, tdir):
    node = FakeNode(tdir, tconf)
    global initial_3pc
    initial_3pc = node.master_replica.last_ordered_3pc
    return node


def check_last_warning(expected_msg):
    global warning_msg_count
    warning_msg_count += 1
    assert warning_msg_count == len(container)
    assert expected_msg in container[-1].msg


def check_3pc_did_not_restore(node):
    assert all(r.last_ordered_3pc == initial_3pc for r in node.replicas.values())


def test_empty_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps(()))
    assert not node._try_restore_last_sent_pre_prepare_seq_no()
    check_last_warning('not enough values to unpack (expected 3, got 0)')
    check_3pc_did_not_restore(node)


def test_empty_string_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps(('')))
    assert not node._try_restore_last_sent_pre_prepare_seq_no()
    check_last_warning('not enough values to unpack (expected 3, got 0)')
    check_3pc_did_not_restore(node)


def test_empty_strings_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps(('', '', '')))
    assert not node._try_restore_last_sent_pre_prepare_seq_no()
    check_last_warning('invalid literal for int() with base 10')
    check_3pc_did_not_restore(node)


def test_empty_none_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps((None)))
    assert not node._try_restore_last_sent_pre_prepare_seq_no()
    check_last_warning('object is not iterable')
    check_3pc_did_not_restore(node)


def test_empty_nones_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps((None, None, None)))
    assert not node._try_restore_last_sent_pre_prepare_seq_no()
    check_last_warning('argument must be a string')
    check_3pc_did_not_restore(node)
