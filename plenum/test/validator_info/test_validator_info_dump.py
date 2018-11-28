import os
import pytest

from plenum.server.node import Node


@pytest.fixture(scope="module")
def tconf(tconf):
    old_val = tconf.VALIDATOR_INFO_USE_DB
    tconf.VALIDATOR_INFO_USE_DB = True
    yield tconf
    tconf.VALIDATOR_INFO_USE_DB = old_val


def test_dump_general_info_use_db(tconf, node):
    node._info_tool.dump_general_info()
    file_name = node._info_tool.GENERAL_FILE_NAME_TEMPLATE.format(node_name=node.name.lower())
    file_path = os.path.join(node.node_info_dir, file_name)
    db_name = node._info_tool.GENERAL_DB_NAME_TEMPLATE.format(node_name=node.name.lower())
    db_path = os.path.join(node.node_info_dir, db_name)
    assert os.path.exists(file_path)
    assert os.path.exists(db_path)


def test_dump_additional_info(node):
    Node.dump_additional_info(node)
    file_name = node._info_tool.ADDITIONAL_FILE_NAME_TEMPLATE.format(node_name=node.name.lower())
    file_path = os.path.join(node.node_info_dir, file_name)
    assert os.path.exists(file_path)


def test_file_version_info(node):
    file_name = node._info_tool.VERSION_FILE_NAME_TEMPLATE.format(node_name=node.name.lower())
    file_path = os.path.join(node.node_info_dir, file_name)
    assert os.path.exists(file_path)
    assert os.path.getsize(file_path) > 0
