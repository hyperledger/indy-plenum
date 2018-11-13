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


@pytest.fixture(scope="module")
def node(tconf, tdir):
    node = FakeNode(tdir, tconf)
    return node


def try_restore_and_check_new_warning(node, expected_msg):
    assert not node._try_restore_last_sent_pre_prepare_seq_no()
    global warning_msg_count
    warning_msg_count += 1
    assert warning_msg_count == len(container)
    assert expected_msg in container[-1].msg


def test_empty_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps(()))
    try_restore_and_check_new_warning(node, 'not enough values to unpack (expected 3, got 0)')


def test_empty_string_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps(('')))
    try_restore_and_check_new_warning(node, 'not enough values to unpack (expected 3, got 0)')


def test_empty_strings_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps(('', '', '')))
    try_restore_and_check_new_warning(node, 'invalid literal for int() with base 10')


def test_empty_none_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps((None)))
    try_restore_and_check_new_warning(node, 'object is not iterable')


def test_empty_nones_value(node):
    node.nodeStatusDB.put(LAST_SENT_PRE_PREPARE, node_status_db_serializer.dumps((None, None, None)))
    try_restore_and_check_new_warning(node, 'argument must be a string')
